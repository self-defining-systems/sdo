package sdk

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ObjectRefError is a rejected agent citation, not an operational failure.
// Compound citations must be supplied as separate evidence entries.
type ObjectRefError struct {
	Code  ObjectRefErrorCode
	Input string
}

type ObjectRefErrorCode string

const (
	ObjectRefMalformed ObjectRefErrorCode = "malformed_object_ref"
	ObjectRefCompound  ObjectRefErrorCode = "compound_object_ref"
)

func (e *ObjectRefError) Error() string {
	return fmt.Sprintf("%s: %q must reference exactly one object: Kind/name or Kind/namespace/name", e.Code, e.Input)
}

var objectKinds = map[string]string{
	"clusterrole": "ClusterRole", "clusterroles": "ClusterRole", "clusterrolebinding": "ClusterRoleBinding", "clusterrolebindings": "ClusterRoleBinding",
	"cm": "ConfigMap", "configmap": "ConfigMap", "configmaps": "ConfigMap",
	"cj": "CronJob", "cronjob": "CronJob", "cronjobs": "CronJob",
	"ds": "DaemonSet", "daemonset": "DaemonSet", "daemonsets": "DaemonSet",
	"deploy": "Deployment", "deployment": "Deployment", "deployments": "Deployment",
	"hpa": "HorizontalPodAutoscaler", "horizontalpodautoscaler": "HorizontalPodAutoscaler", "horizontalpodautoscalers": "HorizontalPodAutoscaler",
	"ing": "Ingress", "ingress": "Ingress", "ingresses": "Ingress",
	"job": "Job", "jobs": "Job", "netpol": "NetworkPolicy", "networkpolicy": "NetworkPolicy", "networkpolicies": "NetworkPolicy",
	"pvc": "PersistentVolumeClaim", "persistentvolumeclaim": "PersistentVolumeClaim", "persistentvolumeclaims": "PersistentVolumeClaim",
	"po": "Pod", "pod": "Pod", "pods": "Pod", "rs": "ReplicaSet", "replicaset": "ReplicaSet", "replicasets": "ReplicaSet",
	"role": "Role", "roles": "Role", "rolebinding": "RoleBinding", "rolebindings": "RoleBinding",
	"secret": "Secret", "secrets": "Secret", "sa": "ServiceAccount", "serviceaccount": "ServiceAccount", "serviceaccounts": "ServiceAccount",
	"svc": "Service", "service": "Service", "services": "Service", "sts": "StatefulSet", "statefulset": "StatefulSet", "statefulsets": "StatefulSet",
}

// CanonicalObjectKind accepts kubectl aliases and group-qualified kinds.
// Unknown kinds remain usable without inventing an API version.
func CanonicalObjectKind(raw string) string {
	kind, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(raw)), ".")
	if canonical, ok := objectKinds[kind]; ok {
		return canonical
	}
	return kind
}

func objectAPIVersion(kind string) string {
	switch kind {
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet":
		return "apps/v1"
	case "Job", "CronJob":
		return "batch/v1"
	case "Ingress", "NetworkPolicy":
		return "networking.k8s.io/v1"
	case "HorizontalPodAutoscaler":
		return "autoscaling/v2"
	case "Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding":
		return "rbac.authorization.k8s.io/v1"
	case "ConfigMap", "Secret", "Pod", "Service", "ServiceAccount", "PersistentVolumeClaim":
		return "v1"
	}
	return ""
}

func refComponent(value string) bool {
	if value == "" {
		return false
	}
	for i, c := range []byte(value) {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		if i > 0 && (c == '.' || c == '-' || c == '_') {
			continue
		}
		return false
	}
	return true
}

// ParseObjectRef is the single grammar for agent object citations. Bare and
// namespaced refs, kubectl aliases and legacy namespace/Kind/name are accepted;
// compound refs produce a typed rejection. An optional apiVersion@ prefix
// preserves explicit versions when formatting a typed ref.
func ParseObjectRef(raw string) (ObjectRef, error) {
	reject := func(code ObjectRefErrorCode) (ObjectRef, error) {
		return ObjectRef{}, &ObjectRefError{Code: code, Input: raw}
	}
	text := strings.TrimSpace(raw)
	if strings.ContainsAny(text, ";,") || len(strings.Fields(text)) > 1 {
		return reject(ObjectRefCompound)
	}
	version := ""
	explicitVersion := false
	if before, after, ok := strings.Cut(text, "@"); ok {
		version, text, explicitVersion = before, after, true
		parts := strings.Split(version, "/")
		if version != "" && (len(parts) > 2 || !refComponent(parts[0]) || (len(parts) == 2 && !refComponent(parts[1]))) {
			return reject(ObjectRefMalformed)
		}
	}
	parts := strings.Split(text, "/")
	if len(parts) != 2 && len(parts) != 3 {
		return reject(ObjectRefMalformed)
	}
	for _, part := range parts {
		if !refComponent(part) {
			return reject(ObjectRefMalformed)
		}
	}
	kind, name, namespace := parts[0], parts[len(parts)-1], ""
	if len(parts) == 3 {
		namespace = parts[1]
		// Resolve the legacy ordering only when its second component is a
		// known kind and the first is not. Never guess between two kinds.
		_, firstKnown := objectKinds[strings.ToLower(strings.SplitN(parts[0], ".", 2)[0])]
		_, secondKnown := objectKinds[strings.ToLower(strings.SplitN(parts[1], ".", 2)[0])]
		if !firstKnown && secondKnown {
			kind, namespace = parts[1], parts[0]
		}
	}
	kind = CanonicalObjectKind(kind)
	if !explicitVersion {
		version = objectAPIVersion(kind)
	}
	return ObjectRef{APIVersion: version, Kind: kind, Namespace: namespace, Name: name}, nil
}

// FormatObjectRef returns canonical text. Non-default API versions are retained
// explicitly, so parsing the formatted value preserves every ObjectRef field.
func FormatObjectRef(ref ObjectRef) (string, error) {
	text := ref.Kind + "/" + ref.Name
	if ref.Namespace != "" {
		text = ref.Kind + "/" + ref.Namespace + "/" + ref.Name
	}
	if ref.APIVersion != objectAPIVersion(ref.Kind) {
		text = ref.APIVersion + "@" + text
	}
	parsed, err := ParseObjectRef(text)
	if err != nil {
		return "", err
	}
	if parsed != ref {
		return "", &ObjectRefError{Code: ObjectRefMalformed, Input: text}
	}
	return text, nil
}

// UnmarshalJSON keeps the SDK's comparable value projection of the protobuf
// ObjectRef. Tool transports may pass raw agent text; Python need not parse it.
func (ref *ObjectRef) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var raw string
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		parsed, err := ParseObjectRef(raw)
		if err != nil {
			return err
		}
		*ref = parsed
		return nil
	}
	type value ObjectRef
	var parsed value
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	*ref = ObjectRef(parsed)
	return nil
}
