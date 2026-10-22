package apihistory

import (
	"encoding/json"
	"errors"
	"strings"
)

// This projection is intentionally independent of controller corpus snapshots.
// Only reviewed identity fields, pagination presence, and key names survive.
// All specs, statuses, labels, annotations, messages and values are opaque.
type responseMetadata struct {
	Name            string `json:"name,omitempty"`
	Namespace       string `json:"namespace,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	Continue        string `json:"continue,omitempty"`
}
type projectedResponse struct {
	APIVersion string              `json:"apiVersion"`
	Kind       string              `json:"kind"`
	Metadata   responseMetadata    `json:"metadata"`
	Redacted   bool                `json:"redacted"`
	Data       map[string]string   `json:"data,omitempty"`
	StringData map[string]string   `json:"stringData,omitempty"`
	BinaryData map[string]string   `json:"binaryData,omitempty"`
	Items      []projectedResponse `json:"items,omitempty"`
}
type responseInput struct {
	APIVersion string                     `json:"apiVersion"`
	Kind       string                     `json:"kind"`
	Metadata   responseMetadata           `json:"metadata"`
	Data       map[string]json.RawMessage `json:"data"`
	StringData map[string]json.RawMessage `json:"stringData"`
	BinaryData map[string]json.RawMessage `json:"binaryData"`
	Items      []json.RawMessage          `json:"items"`
}

func objectGroup(kind string) (string, bool) {
	switch kind {
	case "Pod", "Service", "ConfigMap", "Secret", "Endpoints", "Event":
		return "", true
	case "Deployment", "StatefulSet", "ReplicaSet":
		return "apps", true
	case "HorizontalPodAutoscaler":
		return "autoscaling", true
	case "PodDisruptionBudget":
		return "policy", true
	case "EndpointSlice":
		return "discovery.k8s.io", true
	case "NetworkPolicy":
		return "networking.k8s.io", true
	}
	return "", false
}
func projectResponse(raw []byte) ([]byte, string, error) {
	var input responseInput
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, "", err
	}
	kind := strings.TrimSuffix(input.Kind, "List")
	group, ok := objectGroup(kind)
	// Never persist arbitrary type text from unsupported representations.
	validVersion := input.APIVersion == "v1" && group == ""
	if group != "" {
		validVersion = input.APIVersion == group+"/v1" || (kind == "HorizontalPodAutoscaler" && input.APIVersion == group+"/v2")
	}
	if !ok || !validVersion {
		return nil, "", errors.New("unsupported representation")
	}
	output := projectItem(input, input.Kind, input.APIVersion)
	if strings.HasSuffix(input.Kind, "List") {
		output.Items = make([]projectedResponse, 0, len(input.Items))
		for _, rawItem := range input.Items {
			var item responseInput
			if err := json.Unmarshal(rawItem, &item); err != nil {
				return nil, "", err
			}
			output.Items = append(output.Items, projectItem(item, kind, input.APIVersion))
		}
	}
	b, err := json.Marshal(output)
	return b, input.Metadata.ResourceVersion, err
}
func projectItem(input responseInput, kind, version string) projectedResponse {
	meta := input.Metadata
	if meta.Continue != "" {
		meta.Continue = "redacted"
	}
	output := projectedResponse{APIVersion: version, Kind: kind, Metadata: meta, Redacted: true}
	keys := func(values map[string]json.RawMessage) map[string]string {
		if len(values) == 0 {
			return nil
		}
		out := make(map[string]string, len(values))
		for key := range values {
			out[key] = ""
		}
		return out
	}
	if kind == "Secret" || kind == "ConfigMap" {
		output.Data = keys(input.Data)
		output.StringData = keys(input.StringData)
		output.BinaryData = keys(input.BinaryData)
	}
	return output
}
