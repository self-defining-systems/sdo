package sdk

import (
	"context"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Detector interface {
	Spec() DetectorSpec
	Detect(context.Context, DetectionContext) ([]Finding, error)
}

type DetectorSpec struct {
	ID                  string
	Class               DetectorClass
	Owner               DetectorOwner
	Description         string
	Watches             []WatchKind
	Interval            time.Duration
	Persistence         PersistencePolicy
	Batching            BatchingPolicy
	Playbooks           []string
	OriginatingIncident string
	OriginatingCommit   string
}

type DetectorClass string

const (
	DetectorClassHealth   DetectorClass = "health"
	DetectorClassIncident DetectorClass = "incident"
)

type DetectorOwner string

const (
	DetectorOwnerHealthJudge DetectorOwner = "health_judge"
	DetectorOwnerResponder   DetectorOwner = "responder"
)

type PersistencePolicy struct {
	Firing   int
	Clearing int
	// MinDuration, when positive, additionally requires a finding to have
	// been continuously observed for at least this long before it fires.
	// A finding that reaches Firing evaluations sooner stays pending until
	// MinDuration also elapses, then activates on its next observation. Zero
	// keeps the evaluation-count-only policy. This absorbs findings whose
	// evaluation count crosses the firing threshold within one brief stall
	// (for example a synthetic-traffic window re-evaluated on every poll),
	// while a persistent fault still fires once both conditions hold.
	MinDuration time.Duration
}

type BatchingPolicy struct {
	Severity FindingSeverity
	Debounce time.Duration
}

type WatchKind struct {
	APIVersion string
	Kind       string
	Namespace  string
}

type FindingStatus string

const (
	FindingActive   FindingStatus = "active"
	FindingResolved FindingStatus = "resolved"
)

type FindingSeverity string

const (
	SeverityInfo     FindingSeverity = "info"
	SeverityWarning  FindingSeverity = "warn"
	SeverityCritical FindingSeverity = "critical"
)

type ObjectRef struct {
	APIVersion string `json:"api_version,omitempty"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
}

func ObjectRefFrom(kind string, apiVersion string, object metav1.Object) ObjectRef {
	return ObjectRef{
		APIVersion: apiVersion,
		Kind:       kind,
		Namespace:  object.GetNamespace(),
		Name:       object.GetName(),
	}
}

type Finding struct {
	DetectorID        string               `json:"detector_id,omitempty"`
	RuleID            string               `json:"rule_id"`
	Status            FindingStatus        `json:"status"`
	Severity          FindingSeverity      `json:"severity"`
	Summary           string               `json:"summary"`
	Evidence          string               `json:"evidence"`
	PrimaryResource   ObjectRef            `json:"primary_resource"`
	RelatedResources  []ObjectRef          `json:"related_resources,omitempty"`
	Playbooks         []string             `json:"playbooks,omitempty"`
	ParameterBindings map[string]ObjectRef `json:"parameter_bindings,omitempty"`
	Fingerprint       string               `json:"fingerprint,omitempty"`
	Metadata          map[string]any       `json:"metadata,omitempty"`
}

// ServiceExpectsEndpoints reports whether Kubernetes can back a Service with
// Endpoints, EndpointSlices, or selected pods. ExternalName Services are DNS
// aliases: Kubernetes never gives them endpoints, even when a selector is left
// over from a type change. A detector must not report an ExternalName Service
// for missing ready endpoints or pods.
func ServiceExpectsEndpoints(service corev1.Service) bool {
	return service.Spec.Type != corev1.ServiceTypeExternalName
}

// ConfigMapReference describes a ConfigMap consumed by a Deployment pod
// template. Optional references do not make an absent ConfigMap a fault.
type ConfigMapReference struct {
	Name     string `json:"name"`
	Optional bool   `json:"optional"`
}

// ConfigMapReferencesForDeployment returns the distinct ConfigMaps referenced
// by a Deployment's volumes and container environment. Results are sorted by
// name so detector output does not depend on manifest field ordering. If the
// same ConfigMap is referenced as both optional and required, the required
// reference wins.
func ConfigMapReferencesForDeployment(deployment appsv1.Deployment) []ConfigMapReference {
	references := make(map[string]bool)
	add := func(name string, optional *bool) {
		if name == "" {
			return
		}
		isOptional := optional != nil && *optional
		current, exists := references[name]
		if !exists || current && !isOptional {
			references[name] = isOptional
		}
	}

	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.ConfigMap != nil {
			add(volume.ConfigMap.Name, volume.ConfigMap.Optional)
		}
		if volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.ConfigMap != nil {
				add(source.ConfigMap.Name, source.ConfigMap.Optional)
			}
		}
	}

	addContainerReferences := func(containers []corev1.Container) {
		for _, container := range containers {
			for _, source := range container.EnvFrom {
				if source.ConfigMapRef != nil {
					add(source.ConfigMapRef.Name, source.ConfigMapRef.Optional)
				}
			}
			for _, environment := range container.Env {
				if environment.ValueFrom != nil && environment.ValueFrom.ConfigMapKeyRef != nil {
					reference := environment.ValueFrom.ConfigMapKeyRef
					add(reference.Name, reference.Optional)
				}
			}
		}
	}
	addContainerReferences(deployment.Spec.Template.Spec.InitContainers)
	addContainerReferences(deployment.Spec.Template.Spec.Containers)

	names := make([]string, 0, len(references))
	for name := range references {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]ConfigMapReference, 0, len(names))
	for _, name := range names {
		result = append(result, ConfigMapReference{Name: name, Optional: references[name]})
	}
	return result
}

type DetectionContext interface {
	Namespace() string
	ConfigMaps() []corev1.ConfigMap
	Services() []corev1.Service
	Pods() []corev1.Pod
	Deployments() []appsv1.Deployment
	ReplicaSets() []appsv1.ReplicaSet
	Endpoints() []corev1.Endpoints
	EndpointSlices() []discoveryv1.EndpointSlice
	NetworkPolicies() []networkingv1.NetworkPolicy
	Events() []corev1.Event
	ReadyEndpointCountForService(namespace string, service string) int
	PodsForService(namespace string, service string) []corev1.Pod
	RecentEventsFor(namespace string, kind string, name string) []corev1.Event
}
