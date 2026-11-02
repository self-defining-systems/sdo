package sdk

import (
	"context"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Detector interface {
	Spec() DetectorSpec
	Detect(context.Context, DetectionContext) ([]Finding, error)
}

// WindowedDetector is implemented by detectors that judge a rolling window of
// past observations, so a condition that has just resolved can keep reporting
// until the window rolls over. EvaluationWindow bounds that lag for the
// current view; zero means the detector has no such window.
type WindowedDetector interface {
	EvaluationWindow(DetectionContext) time.Duration
}

// ClearingHorizon is how long after a condition resolves a fresh evaluation of
// the detector may still report it: its rolling window plus the evaluations its
// clearing policy needs at its own cadence.
func ClearingHorizon(detector Detector, view DetectionContext) time.Duration {
	spec := detector.Spec()
	clearing := spec.Persistence.Clearing
	if clearing < 1 {
		clearing = 1
	}
	horizon := time.Duration(clearing) * spec.Interval
	if windowed, ok := detector.(WindowedDetector); ok {
		horizon += windowed.EvaluationWindow(view)
	}
	return horizon
}

type DetectorSpec struct {
	ID          string
	Class       DetectorClass
	Owner       DetectorOwner
	Description string
	Watches     []WatchKind
	Interval    time.Duration
	Persistence PersistencePolicy
	Batching    BatchingPolicy
	// Playbooks are logical playbook IDs (for example "cache-pressure"), never paths.
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
	SeverityLatent   FindingSeverity = "latent"
)

// ObjectRef is the comparable in-process projection of common.proto:ObjectRef.
// Cross-process contracts use the generated protobuf message; raw tool refs
// are parsed only by ParseObjectRef.
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
	DetectorID       string          `json:"detector_id,omitempty"`
	RuleID           string          `json:"rule_id"`
	Status           FindingStatus   `json:"status"`
	Severity         FindingSeverity `json:"severity"`
	Summary          string          `json:"summary"`
	Evidence         string          `json:"evidence"`
	PrimaryResource  ObjectRef       `json:"primary_resource"`
	RelatedResources []ObjectRef     `json:"related_resources,omitempty"`
	// Playbooks are logical playbook IDs.
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
	references := map[string]bool{}
	for _, ref := range PodTemplateReferences(deployment.Spec.Template) {
		if ref.Kind != "ConfigMap" {
			continue
		}
		old, exists := references[ref.Name]
		if !exists || old && !ref.Optional {
			references[ref.Name] = ref.Optional
		}
	}
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

// SourceDiff is the read-only comparison with the accepted source for one resource.
// Missing source or observation evidence is an error, never a clean comparison.
type SourceDiff struct {
	// SourceAbsent is true only after a successful accepted-source lookup finds no object.
	SourceAbsent bool `json:"source_absent,omitempty"`
	// Changed reports declared fields that differ and fields that source does
	// not declare and that were absent when SDO accepted (adopted) the object.
	// Server defaults, fields present at acceptance and kubernetes.io/k8s.io
	// bookkeeping are not changes; nor is a new value of an undeclared field
	// that existed at acceptance. When declared fields agree but the object
	// has no acceptance record for this source, the diff is an error, not
	// unchanged. ReconcileFromSource removes the additions it reports.
	Changed bool `json:"changed"`
	// Text is human-readable evidence for a verdict or finding. Never match
	// on it: a substring of a whole document cannot tell which field or
	// container changed. Compare fields with Field and ContainerChanges.
	Text string `json:"text,omitempty"`
}

// PresentButNotInSource is true while the object exists live but not in accepted source.
// Unlike SourceAbsent it turns false after the delete, so delete-style Diagnose matches on it.
func (d SourceDiff) PresentButNotInSource() bool { return d.Changed && d.SourceAbsent }

// DetectionContext is the frozen snapshot read by detectors and, through
// ReadView, by programs.
type DetectionContext interface {
	StatefulSets() []appsv1.StatefulSet
	DaemonSets() []appsv1.DaemonSet
	Jobs() []batchv1.Job
	CronJobs() []batchv1.CronJob
	PersistentVolumeClaims() []corev1.PersistentVolumeClaim
	ServiceAccounts() []corev1.ServiceAccount
	HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
	PodDisruptionBudgets() []policyv1.PodDisruptionBudget
	Ingresses() []networkingv1.Ingress
	Nodes() []corev1.Node
	Secrets() []SecretMetadata

	// DiffAgainstSource compares a captured resource with accepted source, including
	// source absence. Runtime reads are frozen per snapshot and recorded for replay.
	// DiffAgainstSource compares quantities by value and excludes controller bookkeeping (revision, last-applied and restart annotations, status and managedFields), accepted server defaults, and replicas targeted by an HPA.
	DiffAgainstSource(ref ObjectRef) (SourceDiff, error)
	// PodAdmission returns the frozen dry-run result, false when unobserved.
	PodAdmission() (PodAdmissionResult, bool)
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
