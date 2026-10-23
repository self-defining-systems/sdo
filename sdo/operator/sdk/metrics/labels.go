package metrics

import (
	"crypto/sha256"
	"encoding/hex"

	"sdo.dev/operator/sdk"
)

// Default label names, which match kube-state-metrics and Prometheus
// Kubernetes service discovery.
const (
	DefaultNamespaceLabel  = "namespace"
	DefaultDeploymentLabel = "deployment"
	DefaultPodLabel        = "pod"
	DefaultServiceLabel    = "service"
)

// LabelMapping names the metric labels that identify Kubernetes objects. An
// empty field takes its default.
type LabelMapping struct {
	Namespace  string `json:"namespace,omitempty"`
	Deployment string `json:"deployment,omitempty"`
	Pod        string `json:"pod,omitempty"`
	Service    string `json:"service,omitempty"`
}

func (m LabelMapping) withDefaults() LabelMapping {
	for field, fallback := range map[*string]string{
		&m.Namespace: DefaultNamespaceLabel, &m.Deployment: DefaultDeploymentLabel,
		&m.Pod: DefaultPodLabel, &m.Service: DefaultServiceLabel,
	} {
		if *field == "" {
			*field = fallback
		}
	}
	return m
}

// namespaceOf is the namespace the labels name, when they name one.
func (m LabelMapping) namespaceOf(labels map[string]string) (string, bool) {
	value := labels[m.Namespace]
	return value, value != ""
}

// resources maps labels to Kubernetes objects in namespace. The primary
// resource is the most specific one the labels name (pod, then deployment,
// then service); the others are related. Labels naming none make the
// namespace itself primary.
func (m LabelMapping) resources(namespace string, labels map[string]string) (sdk.ObjectRef, []sdk.ObjectRef) {
	candidates := []sdk.ObjectRef{}
	for _, entry := range []struct {
		label      string
		apiVersion string
		kind       string
	}{
		{m.Pod, "v1", "Pod"},
		{m.Deployment, "apps/v1", "Deployment"},
		{m.Service, "v1", "Service"},
	} {
		if name := labels[entry.label]; name != "" {
			candidates = append(candidates, sdk.ObjectRef{APIVersion: entry.apiVersion, Kind: entry.kind, Namespace: namespace, Name: name})
		}
	}
	if len(candidates) == 0 {
		return sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: namespace}, nil
	}
	return candidates[0], candidates[1:]
}

// fingerprint identifies one labelled series or alert of a rule, so distinct
// series that name the same object stay distinct findings.
func fingerprint(detectorID string, ruleID string, labels map[string]string) string {
	digest := sha256.Sum256([]byte(LabelString(labels)))
	return detectorID + "/" + ruleID + "/" + hex.EncodeToString(digest[:6])
}

// metadataValue is value, or for NaN and infinities their text, so finding
// metadata stays JSON serializable.
func metadataValue(value float64) any {
	text := FormatValue(value)
	if text == "NaN" || text == "+Inf" || text == "-Inf" {
		return text
	}
	return value
}
