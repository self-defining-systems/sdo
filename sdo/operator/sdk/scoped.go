package sdk

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"net/url"
	"sort"
	"strings"
)

const MaxObservationTargets = 32
const MaxObservationObjects = 500

// ObservationTarget is a qualified read grant. Cluster webhooks require one
// explicit name per target; namespace targets cover only the declared kind.
type ObservationTarget struct {
	APIVersion string `json:"api_version" yaml:"api_version"`
	Kind       string `json:"kind" yaml:"kind"`
	Namespace  string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name       string `json:"name,omitempty" yaml:"name,omitempty"`
}

func (t ObservationTarget) Key() string {
	return t.APIVersion + "/" + t.Kind + "/" + t.Namespace + "/" + t.Name
}
func (t ObservationTarget) Cluster() bool {
	return t.Kind == "MutatingWebhookConfiguration" || t.Kind == "ValidatingWebhookConfiguration"
}

// ObservationScope is also the installer ceiling. A zero ceiling authorizes no
// additional observation, regardless of the service account's ambient grants.
type ObservationScope struct {
	Version int                 `json:"version" yaml:"version"`
	Targets []ObservationTarget `json:"targets" yaml:"targets"`
}

func (s ObservationScope) Validate() error {
	if s.Version == 0 && len(s.Targets) == 0 {
		return nil
	}
	if s.Version != 1 || len(s.Targets) > MaxObservationTargets {
		return fmt.Errorf("invalid observation scope version or target cap")
	}
	seen := map[string]bool{}
	for _, t := range s.Targets {
		valid := t.APIVersion == "apps/v1" && t.Kind == "Deployment" || t.APIVersion == "v1" && (t.Kind == "Service" || t.Kind == "Endpoints")
		if t.Cluster() {
			valid = t.APIVersion == "admissionregistration.k8s.io/v1" && t.Namespace == "" && t.Name != "" && len(validation.IsDNS1123Subdomain(t.Name)) == 0
		} else {
			valid = valid && t.Name == "" && t.Namespace != "" && len(validation.IsDNS1123Label(t.Namespace)) == 0
		}
		if !valid || seen[t.Key()] {
			return fmt.Errorf("invalid or duplicate observation target %s", t.Key())
		}
		seen[t.Key()] = true
	}
	return nil
}
func (s ObservationScope) Within(ceiling ObservationScope) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := ceiling.Validate(); err != nil {
		return err
	}
	for _, t := range s.Targets {
		if !ceiling.Allows(t) {
			return fmt.Errorf("observation target exceeds installer ceiling: %s", t.Key())
		}
	}
	return nil
}
func (s ObservationScope) Allows(t ObservationTarget) bool {
	for _, a := range s.Targets {
		if a == t {
			return true
		}
	}
	return false
}
func (s ObservationScope) Identity() string {
	targets := append([]ObservationTarget(nil), s.Targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].Key() < targets[j].Key() })
	s.Targets = targets
	b, _ := json.Marshal(s)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (s ObservationScope) AllowsWatch(w WatchKind, app string) bool {
	if w.Namespace == "" && w.APIVersion == "sdo.dev/v1alpha1" {
		return true
	}
	t := ObservationTarget{APIVersion: w.APIVersion, Kind: w.Kind, Namespace: w.Namespace}
	if !t.Cluster() && (w.Namespace == "" || w.Namespace == app) {
		return true
	}
	for _, a := range s.Targets {
		if a.APIVersion == w.APIVersion && a.Kind == w.Kind && a.Namespace == w.Namespace {
			return true
		}
	}
	return false
}

type ObservationStatus struct {
	Target ObservationTarget `json:"target"`
	State  string            `json:"state"`
	Reason string            `json:"reason,omitempty"`
}

const ObservationObserved = "observed"

// ScopedObservationSource is optional; legacy accessors remain application local.
type ScopedObservationSource interface {
	ScopedDeployments(string) ([]appsv1.Deployment, error)
	ScopedServices(string) ([]corev1.Service, error)
	ScopedEndpoints(string) ([]corev1.Endpoints, error)
	MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error)
	ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error)
	ObservationStatuses() []ObservationStatus
	ObservationGeneration() string
}

// ScopedSnapshot is the versioned, credential-free replay representation.
// Accessors project again so even manually constructed replay fixtures cannot
// accidentally expose fields excluded by the public scoped API.
type ScopedSnapshot struct {
	Version     int                                          `json:"version"`
	Scope       ObservationScope                             `json:"scope"`
	Generation  string                                       `json:"generation"`
	Statuses    []ObservationStatus                          `json:"statuses"`
	Deployments []appsv1.Deployment                          `json:"deployments,omitempty"`
	Services    []corev1.Service                             `json:"services,omitempty"`
	Endpoints   []corev1.Endpoints                           `json:"endpoints,omitempty"`
	Mutating    []admissionv1.MutatingWebhookConfiguration   `json:"mutating_webhooks,omitempty"`
	Validating  []admissionv1.ValidatingWebhookConfiguration `json:"validating_webhooks,omitempty"`
}

func (s ScopedSnapshot) ObservationStatuses() []ObservationStatus {
	out := append([]ObservationStatus(nil), s.Statuses...)
	sort.Slice(out, func(i, j int) bool { return out[i].Target.Key() < out[j].Target.Key() })
	return out
}
func (s ScopedSnapshot) check(api, kind, ns string) error {
	if err := s.ValidateObservation(); err != nil {
		return err
	}
	found := false
	for _, t := range s.Scope.Targets {
		if t.APIVersion != api || t.Kind != kind || t.Namespace != ns {
			continue
		}
		found = true
		observed := false
		for _, status := range s.Statuses {
			if status.Target == t && status.State == ObservationObserved {
				observed = true
			}
		}
		if !observed {
			return fmt.Errorf("scoped observation unavailable: %s", t.Key())
		}
	}
	if !found {
		return fmt.Errorf("scoped observation undeclared: %s/%s/%s", api, kind, ns)
	}
	return nil
}
func (s ScopedSnapshot) ScopedDeployments(ns string) ([]appsv1.Deployment, error) {
	if err := s.check("apps/v1", "Deployment", ns); err != nil {
		return nil, err
	}
	out := []appsv1.Deployment{}
	for _, o := range s.Deployments {
		if o.Namespace == ns {
			out = append(out, ProjectForeignDeployment(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (s ScopedSnapshot) ScopedServices(ns string) ([]corev1.Service, error) {
	if err := s.check("v1", "Service", ns); err != nil {
		return nil, err
	}
	out := []corev1.Service{}
	for _, o := range s.Services {
		if o.Namespace == ns {
			out = append(out, ProjectForeignService(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (s ScopedSnapshot) ScopedEndpoints(ns string) ([]corev1.Endpoints, error) {
	if err := s.check("v1", "Endpoints", ns); err != nil {
		return nil, err
	}
	out := []corev1.Endpoints{}
	for _, o := range s.Endpoints {
		if o.Namespace == ns {
			out = append(out, ProjectForeignEndpoints(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (s ScopedSnapshot) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	if err := s.check("admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration", ""); err != nil {
		return nil, err
	}
	out := []admissionv1.MutatingWebhookConfiguration{}
	for _, o := range s.Mutating {
		if s.Scope.Allows(ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: o.Name}) {
			out = append(out, ProjectMutatingWebhook(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (s ScopedSnapshot) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	if err := s.check("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration", ""); err != nil {
		return nil, err
	}
	out := []admissionv1.ValidatingWebhookConfiguration{}
	for _, o := range s.Validating {
		if s.Scope.Allows(ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: o.Name}) {
			out = append(out, ProjectValidatingWebhook(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func safeMeta(m metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace}
}

// Foreign projections use an allowlist. No environment, commands, arguments,
// annotations, probes, volumes, Secret references or arbitrary status messages.
func ProjectForeignDeployment(o appsv1.Deployment) appsv1.Deployment {
	out := appsv1.Deployment{ObjectMeta: safeMeta(o.ObjectMeta)}
	if o.Spec.Replicas == nil {
		v := int32(1)
		out.Spec.Replicas = &v
	}
	if o.Spec.Replicas != nil {
		v := *o.Spec.Replicas
		out.Spec.Replicas = &v
	}
	out.Spec.Selector = o.Spec.Selector.DeepCopy()
	out.Spec.Template.Labels = copyStrings(o.Spec.Template.Labels)
	for _, c := range o.Spec.Template.Spec.Containers {
		out.Spec.Template.Spec.Containers = append(out.Spec.Template.Spec.Containers, corev1.Container{Name: c.Name, Image: safeImage(c.Image), Ports: append([]corev1.ContainerPort(nil), c.Ports...), Resources: *c.Resources.DeepCopy()})
	}
	for i := range out.Spec.Template.Spec.Containers {
		for j := range out.Spec.Template.Spec.Containers[i].Ports {
			port := &out.Spec.Template.Spec.Containers[i].Ports[j]
			if port.Protocol == "" {
				port.Protocol = corev1.ProtocolTCP
			}
		}
	}
	out.Status.Replicas = o.Status.Replicas
	out.Status.ReadyReplicas = o.Status.ReadyReplicas
	out.Status.AvailableReplicas = o.Status.AvailableReplicas
	out.Status.UpdatedReplicas = o.Status.UpdatedReplicas
	return out
}
func safeImage(value string) string {
	if strings.ContainsAny(value, "?#") || strings.Contains(value, "://") {
		return "[redacted]"
	}
	return value
}
func copyStrings(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func ProjectForeignService(o corev1.Service) corev1.Service {
	out := corev1.Service{ObjectMeta: safeMeta(o.ObjectMeta)}
	out.Spec.Selector = copyStrings(o.Spec.Selector)
	out.Spec.Type = o.Spec.Type
	if out.Spec.Type == "" {
		out.Spec.Type = corev1.ServiceTypeClusterIP
	}
	out.Spec.ClusterIP = o.Spec.ClusterIP
	out.Spec.ExternalName = o.Spec.ExternalName
	out.Spec.Ports = make([]corev1.ServicePort, len(o.Spec.Ports))
	for i := range o.Spec.Ports {
		out.Spec.Ports[i] = *o.Spec.Ports[i].DeepCopy()
		defaultServicePort(&out.Spec.Ports[i])
	}
	return out
}
func ProjectForeignEndpoints(o corev1.Endpoints) corev1.Endpoints {
	out := corev1.Endpoints{ObjectMeta: safeMeta(o.ObjectMeta)}
	for _, s := range o.Subsets {
		part := corev1.EndpointSubset{Ports: make([]corev1.EndpointPort, len(s.Ports))}
		for i := range s.Ports {
			part.Ports[i] = *s.Ports[i].DeepCopy()
			if part.Ports[i].Protocol == "" {
				part.Ports[i].Protocol = corev1.ProtocolTCP
			}
		}
		for _, a := range s.Addresses {
			part.Addresses = append(part.Addresses, corev1.EndpointAddress{IP: a.IP, Hostname: a.Hostname})
		}
		for _, a := range s.NotReadyAddresses {
			part.NotReadyAddresses = append(part.NotReadyAddresses, corev1.EndpointAddress{IP: a.IP, Hostname: a.Hostname})
		}
		sort.Slice(part.Addresses, func(i, j int) bool { return part.Addresses[i].IP < part.Addresses[j].IP })
		sort.Slice(part.NotReadyAddresses, func(i, j int) bool { return part.NotReadyAddresses[i].IP < part.NotReadyAddresses[j].IP })
		sort.Slice(part.Ports, func(i, j int) bool { return part.Ports[i].Name < part.Ports[j].Name })
		out.Subsets = append(out.Subsets, part)
	}
	sort.Slice(out.Subsets, func(i, j int) bool {
		a, _ := json.Marshal(out.Subsets[i])
		b, _ := json.Marshal(out.Subsets[j])
		return string(a) < string(b)
	})
	return out
}
func projectClient(c admissionv1.WebhookClientConfig) admissionv1.WebhookClientConfig {
	out := admissionv1.WebhookClientConfig{}
	if c.Service != nil {
		out.Service = c.Service.DeepCopy()
		out.Service.Path = nil
		if out.Service.Port == nil {
			v := int32(443)
			out.Service.Port = &v
		}
	}
	if c.URL != nil {
		v := "[redacted external URL]"
		if parsed, err := url.Parse(*c.URL); err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http") && parsed.Host != "" {
			parsed.User = nil
			parsed.RawQuery = ""
			parsed.ForceQuery = false
			parsed.Path = ""
			parsed.RawPath = ""
			parsed.Fragment = ""
			parsed.RawFragment = ""
			v = parsed.String()
		}
		out.URL = &v
	}
	if len(c.CABundle) > 0 {
		digest := false
		if len(c.CABundle) == 71 && strings.HasPrefix(string(c.CABundle), "sha256:") {
			_, err := hex.DecodeString(string(c.CABundle[7:]))
			digest = err == nil
		}
		if digest {
			out.CABundle = append([]byte(nil), c.CABundle...)
		} else {
			h := sha256.Sum256(c.CABundle)
			out.CABundle = []byte("sha256:" + hex.EncodeToString(h[:]))
		}
	}
	return out
}
func ProjectMutatingWebhook(o admissionv1.MutatingWebhookConfiguration) admissionv1.MutatingWebhookConfiguration {
	out := *o.DeepCopy()
	out.ObjectMeta = safeMeta(o.ObjectMeta)
	for i := range out.Webhooks {
		w := &out.Webhooks[i]
		defaultMutatingWebhook(w)
		w.ClientConfig = projectClient(w.ClientConfig)
		w.MatchConditions = nil
	}
	return out
}
func ProjectValidatingWebhook(o admissionv1.ValidatingWebhookConfiguration) admissionv1.ValidatingWebhookConfiguration {
	out := *o.DeepCopy()
	out.ObjectMeta = safeMeta(o.ObjectMeta)
	for i := range out.Webhooks {
		w := &out.Webhooks[i]
		defaultValidatingWebhook(w)
		w.ClientConfig = projectClient(w.ClientConfig)
		w.MatchConditions = nil
	}
	return out
}

func (s ScopedSnapshot) ObservationGeneration() string { return s.Generation }

// ScopedSource returns a failing source when an optional interface is absent.
func ScopedSource(view any) ScopedObservationSource {
	if source, ok := view.(ScopedObservationSource); ok {
		return source
	}
	return ScopedSnapshot{}
}

// ValidateObservation checks the persisted replay version and coverage envelope.
func (s ScopedSnapshot) ValidateObservation() error {
	if s.Version == 0 && len(s.Scope.Targets) == 0 && len(s.Statuses) == 0 {
		return nil
	}
	if s.Version != 1 {
		return fmt.Errorf("unsupported scoped observation version")
	}
	if err := s.Scope.Validate(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, status := range s.Statuses {
		if !s.Scope.Allows(status.Target) || seen[status.Target.Key()] {
			return fmt.Errorf("invalid scoped observation coverage")
		}
		seen[status.Target.Key()] = true
		switch status.State {
		case ObservationObserved, "forbidden", "unsupported", "unsynced", "unavailable":
		default:
			return fmt.Errorf("invalid scoped availability state")
		}
	}
	if len(seen) != len(s.Scope.Targets) {
		return fmt.Errorf("incomplete scoped observation coverage")
	}
	return nil
}
