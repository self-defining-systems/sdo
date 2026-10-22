package apihistory

import (
	"encoding/json"
	"errors"
	"strings"
)

// History has its own positive allowlist, independent of controller snapshots.
// A reviewed leaf accepts only its JSON scalar type; objects and arrays never
// pass through wholesale. Adding a Kubernetes API field cannot widen storage.
// Omitted fields remain opaque under the root redacted marker.
type projection func(any) (any, bool)
type projectionFields map[string]projection

func textField(v any) (any, bool)   { s, ok := v.(string); return s, ok }
func numberField(v any) (any, bool) { n, ok := v.(float64); return n, ok }
func boolField(v any) (any, bool)   { b, ok := v.(bool); return b, ok }
func numberOrText(v any) (any, bool) {
	if s, ok := textField(v); ok {
		return s, true
	}
	return numberField(v)
}
func object(fields projectionFields) projection {
	return func(v any) (any, bool) {
		input, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		output := make(map[string]any)
		for name, field := range fields {
			if raw, exists := input[name]; exists {
				if value, ok := field(raw); ok {
					output[name] = value
				}
			}
		}
		return output, true
	}
}
func array(item projection) projection {
	return func(v any) (any, bool) {
		input, ok := v.([]any)
		if !ok {
			return nil, false
		}
		output := make([]any, 0, len(input))
		for _, raw := range input {
			if value, ok := item(raw); ok {
				output = append(output, value)
			}
		}
		return output, true
	}
}

// Label/selector values and Secret/ConfigMap payloads are never retained.
func keyNames(v any) (any, bool) {
	input, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	output := make(map[string]any, len(input))
	for key := range input {
		output[key] = ""
	}
	return output, true
}
func presence(fields projectionFields, source, target string) projection {
	base := object(fields)
	return func(v any) (any, bool) {
		value, ok := base(v)
		if !ok {
			return nil, false
		}
		input := v.(map[string]any)
		if raw, exists := input[source]; exists {
			if s, ok := raw.(string); ok {
				value.(map[string]any)[target] = s != ""
			}
		}
		return value, true
	}
}

var labelSelector = object(projectionFields{
	"matchLabels":      keyNames,
	"matchExpressions": array(object(projectionFields{"key": textField, "operator": textField})),
})
var conditionFields = array(object(projectionFields{"type": textField, "status": textField, "reason": textField}))
var referenceFields = object(projectionFields{"name": textField, "key": textField, "optional": boolField})
var portFields = array(object(projectionFields{"name": textField, "port": numberField, "protocol": textField}))
var probeFields = object(projectionFields{
	"httpGet":             object(projectionFields{"path": textField, "port": numberOrText, "scheme": textField}),
	"tcpSocket":           object(projectionFields{"port": numberOrText}),
	"grpc":                object(projectionFields{"port": numberField}),
	"exec":                object(projectionFields{}), // Handler presence only, never its command.
	"initialDelaySeconds": numberField, "timeoutSeconds": numberField, "periodSeconds": numberField,
	"successThreshold": numberField, "failureThreshold": numberField, "terminationGracePeriodSeconds": numberField,
})

// CPU/memory/storage are reviewed resource names; arbitrary extended-resource
// names and future resource-map values do not enter this projection.
var resourceQuantities = object(projectionFields{"cpu": numberOrText, "memory": numberOrText, "ephemeral-storage": numberOrText, "storage": numberOrText})
var containerFields = array(object(projectionFields{
	"name": textField, "image": textField,
	"ports":          array(object(projectionFields{"name": textField, "containerPort": numberField, "hostPort": numberField, "protocol": textField})),
	"readinessProbe": probeFields, "livenessProbe": probeFields, "startupProbe": probeFields,
	"resources": object(projectionFields{"requests": resourceQuantities, "limits": resourceQuantities}),
	"env": array(object(projectionFields{"name": textField, "valueFrom": object(projectionFields{
		"secretKeyRef": referenceFields, "configMapKeyRef": referenceFields,
		"fieldRef":         object(projectionFields{"apiVersion": textField, "fieldPath": textField}),
		"resourceFieldRef": object(projectionFields{"containerName": textField, "resource": textField, "divisor": numberOrText}),
	})})),
	"envFrom":      array(object(projectionFields{"secretRef": referenceFields, "configMapRef": referenceFields})),
	"volumeMounts": array(object(projectionFields{"name": textField})),
}))
var podSpecFields = object(projectionFields{
	"containers": containerFields, "initContainers": containerFields, "nodeName": textField,
	"volumes": array(object(projectionFields{"name": textField})),
})
var templateFields = object(projectionFields{"metadata": object(projectionFields{"labels": keyNames}), "spec": podSpecFields})
var workloadStatus = object(projectionFields{
	"replicas": numberField, "readyReplicas": numberField, "availableReplicas": numberField,
	"unavailableReplicas": numberField, "updatedReplicas": numberField, "currentReplicas": numberField,
	"fullyLabeledReplicas": numberField, "conditions": conditionFields,
})
var containerState = object(projectionFields{
	"waiting":    object(projectionFields{"reason": textField}),
	"running":    object(projectionFields{}),
	"terminated": object(projectionFields{"reason": textField, "exitCode": numberField}),
})
var containerStatuses = array(object(projectionFields{
	"name": textField, "image": textField, "ready": boolField, "restartCount": numberField,
	"state": containerState, "lastState": containerState,
}))
var objectReference = object(projectionFields{"kind": textField, "namespace": textField, "name": textField, "apiVersion": textField})
var metricTarget = object(projectionFields{"type": textField, "value": numberOrText, "averageValue": numberOrText, "averageUtilization": numberField})
var metricIdentifier = object(projectionFields{"name": textField, "selector": labelSelector})
var scalingRules = object(projectionFields{
	"stabilizationWindowSeconds": numberField, "selectPolicy": textField,
	"policies": array(object(projectionFields{"type": textField, "value": numberField, "periodSeconds": numberField})),
})
var metricFields = array(object(projectionFields{
	"type":              textField,
	"resource":          object(projectionFields{"name": textField, "target": metricTarget}),
	"containerResource": object(projectionFields{"name": textField, "container": textField, "target": metricTarget}),
	"pods":              object(projectionFields{"metric": metricIdentifier, "target": metricTarget}),
	"object":            object(projectionFields{"describedObject": objectReference, "metric": metricIdentifier, "target": metricTarget}),
	"external":          object(projectionFields{"metric": metricIdentifier, "target": metricTarget}),
}))
var networkPeers = array(object(projectionFields{
	"podSelector": labelSelector, "namespaceSelector": labelSelector,
	"ipBlock": object(projectionFields{"cidr": textField, "except": array(textField)}),
}))
var networkPorts = array(object(projectionFields{"protocol": textField, "port": numberOrText, "endPort": numberField}))

func endpointSubset(v any) (any, bool) {
	value, ok := object(projectionFields{"ports": portFields})(v)
	if !ok {
		return nil, false
	}
	for source, target := range map[string]string{"addresses": "readyAddressCount", "notReadyAddresses": "notReadyAddressCount"} {
		if addresses, ok := v.(map[string]any)[source].([]any); ok {
			value.(map[string]any)[target] = len(addresses)
		}
	}
	return value, true
}
func endpointSliceCounts(v any) (any, bool) {
	endpoints, ok := v.([]any)
	if !ok {
		return nil, false
	}
	ready, notReady := 0, 0
	for _, raw := range endpoints {
		endpoint, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		addresses, ok := endpoint["addresses"].([]any)
		if !ok {
			continue
		}
		conditions, _ := endpoint["conditions"].(map[string]any)
		// Kubernetes treats an unspecified readiness condition as ready.
		if conditions["ready"] == false {
			notReady += len(addresses)
		} else {
			ready += len(addresses)
		}
	}
	return map[string]any{"readyAddressCount": ready, "notReadyAddressCount": notReady}, true
}

// Every retained nested field is enumerated here or in the shared shapes above.
var operationalFields = map[string]projectionFields{
	"Service": {"spec": presence(projectionFields{
		"type": textField, "selector": keyNames,
		"ports": array(object(projectionFields{"name": textField, "port": numberField, "targetPort": numberOrText, "protocol": textField, "nodePort": numberField})),
	}, "clusterIP", "clusterIPPresent")},
	"Deployment": {"spec": object(projectionFields{
		"replicas": numberField, "strategy": object(projectionFields{"type": textField}), "selector": labelSelector, "template": templateFields,
	}), "status": workloadStatus},
	"StatefulSet": {"spec": object(projectionFields{
		"replicas": numberField, "updateStrategy": object(projectionFields{"type": textField}), "selector": labelSelector, "template": templateFields,
	}), "status": workloadStatus},
	"ReplicaSet": {"spec": object(projectionFields{"replicas": numberField, "selector": labelSelector, "template": templateFields}), "status": workloadStatus},
	"Pod": {"spec": podSpecFields, "status": presence(projectionFields{
		"phase": textField, "conditions": conditionFields, "containerStatuses": containerStatuses, "initContainerStatuses": containerStatuses,
	}, "podIP", "podIPPresent")},
	"Endpoints":     {"subsets": array(endpointSubset)},
	"EndpointSlice": {"ports": portFields},
	"Event": {"type": textField, "reason": textField, "involvedObject": objectReference, "count": numberField,
		"firstTimestamp": textField, "lastTimestamp": textField, "eventTime": textField,
		"series": object(projectionFields{"count": numberField, "lastObservedTime": textField})},
	"HorizontalPodAutoscaler": {"spec": object(projectionFields{
		"scaleTargetRef": objectReference, "minReplicas": numberField, "maxReplicas": numberField,
		"targetCPUUtilizationPercentage": numberField, // autoscaling/v1
		"metrics":                        metricFields, "behavior": object(projectionFields{"scaleUp": scalingRules, "scaleDown": scalingRules}),
	})},
	"PodDisruptionBudget": {"spec": object(projectionFields{
		"minAvailable": numberOrText, "maxUnavailable": numberOrText, "selector": labelSelector, "unhealthyPodEvictionPolicy": textField,
	})},
	"NetworkPolicy": {"spec": object(projectionFields{
		"podSelector": labelSelector, "policyTypes": array(textField),
		"ingress": array(object(projectionFields{"from": networkPeers, "ports": networkPorts})),
		"egress":  array(object(projectionFields{"to": networkPeers, "ports": networkPorts})),
	})},
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
	var input map[string]any
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, "", err
	}
	inputKind, _ := input["kind"].(string)
	version, _ := input["apiVersion"].(string)
	kind := strings.TrimSuffix(inputKind, "List")
	group, ok := objectGroup(kind)
	validVersion := version == "v1" && group == ""
	if group != "" {
		validVersion = version == group+"/v1" || (kind == "HorizontalPodAutoscaler" && version == group+"/v2")
	}
	if !ok || !validVersion {
		return nil, "", errors.New("unsupported representation")
	}
	output := projectItem(input, inputKind, version)
	if strings.HasSuffix(inputKind, "List") {
		items, _ := input["items"].([]any)
		projected := make([]any, 0, len(items))
		for _, rawItem := range items {
			item, ok := rawItem.(map[string]any)
			if !ok {
				return nil, "", errors.New("invalid list item")
			}
			projected = append(projected, projectItem(item, kind, version))
		}
		output["items"] = projected
	}
	metadata, _ := input["metadata"].(map[string]any)
	rv, _ := metadata["resourceVersion"].(string)
	b, err := json.Marshal(output)
	return b, rv, err
}
func projectItem(input map[string]any, kind, version string) map[string]any {
	metaFields := projectionFields{"name": textField, "namespace": textField, "resourceVersion": textField}
	if kind != "Secret" && kind != "ConfigMap" && !strings.HasSuffix(kind, "List") {
		metaFields["labels"] = keyNames
	}
	if kind == "Pod" {
		metaFields["ownerReferences"] = array(object(projectionFields{"kind": textField, "name": textField}))
	}
	metadata := map[string]any{}
	if value, ok := object(metaFields)(input["metadata"]); ok {
		metadata = value.(map[string]any)
	}
	if original, ok := input["metadata"].(map[string]any); ok {
		if token, ok := original["continue"].(string); ok && token != "" {
			metadata["continue"] = "redacted"
		}
	}
	value, _ := object(operationalFields[kind])(input)
	output := value.(map[string]any)
	output["apiVersion"], output["kind"], output["metadata"], output["redacted"] = version, kind, metadata, true
	if kind == "Secret" || kind == "ConfigMap" {
		for _, name := range []string{"data", "stringData", "binaryData"} {
			if keys, ok := keyNames(input[name]); ok && len(keys.(map[string]any)) > 0 {
				output[name] = keys
			}
		}
	}
	if kind == "EndpointSlice" {
		if counts, ok := endpointSliceCounts(input["endpoints"]); ok {
			for name, value := range counts.(map[string]any) {
				output[name] = value
			}
		}
	}
	return output
}
