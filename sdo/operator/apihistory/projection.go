package apihistory

import (
	"encoding/json"
	"errors"
	"strings"
)

// History has its own positive allowlist, independent of controller snapshots.
// Identifiers use the documented disclosure policy; constrained string leaves
// are validated by projection_leaves.go. Objects and arrays never
// pass through wholesale. Adding a Kubernetes API field cannot widen storage.
// Omitted fields remain opaque under the root redacted marker.
type projection func(any) (any, bool)
type projectionFields map[string]projection

func numberField(v any) (any, bool) { n, ok := v.(float64); return n, ok }
func boolField(v any) (any, bool)   { b, ok := v.(bool); return b, ok }
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
	"matchLabels":      identifierKeys("selector.key"),
	"matchExpressions": array(object(projectionFields{"key": identifierField("selector.key"), "operator": enumField("In", "NotIn", "Exists", "DoesNotExist")})),
})

func conditions(typeField projection) projection {
	return array(object(projectionFields{"type": typeField, "status": enumField("True", "False", "Unknown"), "reason": reasonField}))
}

var referenceFields = object(projectionFields{"name": identifierField("reference.name"), "key": identifierField("reference.key"), "optional": boolField})
var bulkReferenceFields = object(projectionFields{"name": identifierField("reference.name"), "optional": boolField})
var portFields = array(object(projectionFields{"name": identifierField("endpoint.port.name"), "port": numberField, "protocol": protocolField}))

// HTTP request targets are opaque, including pathname segments, queries and
// fragments. Keep handler kind, port, scheme and timing only.
var probeFields = object(projectionFields{
	"httpGet":             object(projectionFields{"port": portField, "scheme": enumField("HTTP", "HTTPS")}),
	"tcpSocket":           object(projectionFields{"port": portField}),
	"grpc":                object(projectionFields{"port": numberField}),
	"exec":                object(projectionFields{}), // Handler presence only, never its command.
	"initialDelaySeconds": numberField, "timeoutSeconds": numberField, "periodSeconds": numberField,
	"successThreshold": numberField, "failureThreshold": numberField, "terminationGracePeriodSeconds": numberField,
})

// CPU/memory/storage are reviewed resource names; arbitrary extended-resource
// names and future resource-map values do not enter this projection.
var resourceQuantities = object(projectionFields{"cpu": quantityField, "memory": quantityField, "ephemeral-storage": quantityField, "storage": quantityField})
var containerFields = array(object(projectionFields{
	"name": identifierField("container.name"), "image": imageField,
	"ports":          array(object(projectionFields{"name": identifierField("container.port.name"), "containerPort": numberField, "hostPort": numberField, "protocol": protocolField})),
	"readinessProbe": probeFields, "livenessProbe": probeFields, "startupProbe": probeFields,
	"resources": object(projectionFields{"requests": resourceQuantities, "limits": resourceQuantities}),
	"env": array(object(projectionFields{"name": identifierField("env.name"), "valueFrom": object(projectionFields{
		"secretKeyRef": referenceFields, "configMapKeyRef": referenceFields,
		"fieldRef":         object(projectionFields{"apiVersion": enumField("v1"), "fieldPath": fieldPathField}),
		"resourceFieldRef": object(projectionFields{"containerName": identifierField("container.name"), "resource": resourceField, "divisor": quantityField}),
	})})),
	"envFrom":      array(object(projectionFields{"secretRef": bulkReferenceFields, "configMapRef": bulkReferenceFields})),
	"volumeMounts": array(object(projectionFields{"name": identifierField("volume.name")})),
}))
var podSpecFields = object(projectionFields{
	"containers": containerFields, "initContainers": containerFields, "nodeName": identifierField("node.name"),
	"volumes": array(object(projectionFields{"name": identifierField("volume.name")})),
})
var templateFields = object(projectionFields{"metadata": object(projectionFields{"labels": identifierKeys("label.key")}), "spec": podSpecFields})
var workloadStatus = object(projectionFields{
	"replicas": numberField, "readyReplicas": numberField, "availableReplicas": numberField,
	"unavailableReplicas": numberField, "updatedReplicas": numberField, "currentReplicas": numberField,
	"fullyLabeledReplicas": numberField, "conditions": conditions(workloadConditionType),
})
var containerState = object(projectionFields{
	"waiting":    object(projectionFields{"reason": reasonField}),
	"running":    object(projectionFields{}),
	"terminated": object(projectionFields{"reason": reasonField, "exitCode": numberField}),
})
var containerStatuses = array(object(projectionFields{
	"name": identifierField("container.name"), "image": imageField, "ready": boolField, "restartCount": numberField,
	"state": containerState, "lastState": containerState,
}))
var crossVersionReference = projectionFields{"kind": identifierField("reference.kind"), "name": identifierField("reference.name"), "apiVersion": identifierField("reference.apiVersion")}
var objectReference = object(projectionFields{"kind": identifierField("reference.kind"), "namespace": identifierField("reference.namespace"), "name": identifierField("reference.name"), "apiVersion": identifierField("reference.apiVersion")})
var metricTarget = object(projectionFields{"type": enumField("Utilization", "Value", "AverageValue"), "value": quantityField, "averageValue": quantityField, "averageUtilization": numberField})
var metricIdentifier = object(projectionFields{"name": identifierField("metric.name"), "selector": labelSelector})
var scalingRules = object(projectionFields{
	"stabilizationWindowSeconds": numberField, "selectPolicy": enumField("Max", "Min", "Disabled"),
	"policies": array(object(projectionFields{"type": enumField("Pods", "Percent"), "value": numberField, "periodSeconds": numberField})),
})
var metricFields = array(object(projectionFields{
	"type":              enumField("Resource", "ContainerResource", "Pods", "Object", "External"),
	"resource":          object(projectionFields{"name": identifierField("resource.name"), "target": metricTarget}),
	"containerResource": object(projectionFields{"name": identifierField("resource.name"), "container": identifierField("container.name"), "target": metricTarget}),
	"pods":              object(projectionFields{"metric": metricIdentifier, "target": metricTarget}),
	"object":            object(projectionFields{"describedObject": object(crossVersionReference), "metric": metricIdentifier, "target": metricTarget}),
	"external":          object(projectionFields{"metric": metricIdentifier, "target": metricTarget}),
}))
var networkPeers = array(object(projectionFields{
	"podSelector": labelSelector, "namespaceSelector": labelSelector,
	"ipBlock": object(projectionFields{"cidr": cidrField, "except": array(cidrField)}),
}))
var networkPorts = array(object(projectionFields{"protocol": protocolField, "port": portField, "endPort": numberField}))

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
		"type": enumField("ClusterIP", "NodePort", "LoadBalancer", "ExternalName"), "selector": identifierKeys("selector.key"),
		"ports": array(object(projectionFields{"name": identifierField("service.port.name"), "port": numberField, "targetPort": portField, "protocol": protocolField, "nodePort": numberField})),
	}, "clusterIP", "clusterIPPresent")},
	"Deployment": {"spec": object(projectionFields{
		"replicas": numberField, "strategy": object(projectionFields{"type": enumField("Recreate", "RollingUpdate")}), "selector": labelSelector, "template": templateFields,
	}), "status": workloadStatus},
	"StatefulSet": {"spec": object(projectionFields{
		"replicas": numberField, "updateStrategy": object(projectionFields{"type": enumField("OnDelete", "RollingUpdate")}), "selector": labelSelector, "template": templateFields,
	}), "status": workloadStatus},
	"ReplicaSet": {"spec": object(projectionFields{"replicas": numberField, "selector": labelSelector, "template": templateFields}), "status": workloadStatus},
	"Pod": {"spec": podSpecFields, "status": presence(projectionFields{
		"phase": enumField("Pending", "Running", "Succeeded", "Failed", "Unknown"), "conditions": conditions(identifierField("Pod.condition.type")), "containerStatuses": containerStatuses, "initContainerStatuses": containerStatuses,
	}, "podIP", "podIPPresent")},
	"Endpoints":     {"subsets": array(endpointSubset)},
	"EndpointSlice": {"ports": portFields},
	"Event": {"type": enumField("Normal", "Warning"), "reason": reasonField, "involvedObject": objectReference, "count": numberField,
		"firstTimestamp": timestampField, "lastTimestamp": timestampField, "eventTime": timestampField,
		"series": object(projectionFields{"count": numberField, "lastObservedTime": timestampField})},
	"HorizontalPodAutoscaler": {"spec": object(projectionFields{
		"scaleTargetRef": object(crossVersionReference), "minReplicas": numberField, "maxReplicas": numberField,
		"targetCPUUtilizationPercentage": numberField, // autoscaling/v1
		"metrics":                        metricFields, "behavior": object(projectionFields{"scaleUp": scalingRules, "scaleDown": scalingRules}),
	})},
	"PodDisruptionBudget": {"spec": object(projectionFields{
		"minAvailable": percentOrNumber, "maxUnavailable": percentOrNumber, "selector": labelSelector, "unhealthyPodEvictionPolicy": enumField("IfHealthyBudget", "AlwaysAllow"),
	})},
	"NetworkPolicy": {"spec": object(projectionFields{
		"podSelector": labelSelector, "policyTypes": array(enumField("Ingress", "Egress")),
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

// Malformed identity/pagination must not turn into a valid empty list that a
// history reader could mistake for an observation of absence.
func validIdentity(input map[string]any) bool {
	raw := input["metadata"]
	if raw == nil {
		return true
	}
	metadata, ok := raw.(map[string]any)
	if !ok {
		return false
	}
	for _, key := range []string{"name", "namespace", "resourceVersion", "continue"} {
		if value := metadata[key]; value != nil {
			if _, ok := value.(string); !ok {
				return false
			}
		}
	}
	return true
}

func projectResponse(raw []byte) ([]byte, string, error) {
	return projectResponseWithVersion(raw, identifierField("metadata.resourceVersion"))
}

func projectResponseWithVersion(raw []byte, resourceVersion projection) ([]byte, string, error) {
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
	if !validIdentity(input) {
		return nil, "", errors.New("invalid object metadata")
	}
	output := projectItem(input, inputKind, version, resourceVersion)
	if strings.HasSuffix(inputKind, "List") {
		items, valid := input["items"].([]any)
		if input["items"] != nil && !valid {
			return nil, "", errors.New("invalid list items")
		}
		projected := make([]any, 0, len(items))
		for _, rawItem := range items {
			item, ok := rawItem.(map[string]any)
			if !ok || !validIdentity(item) {
				return nil, "", errors.New("invalid list item")
			}
			projected = append(projected, projectItem(item, kind, version, resourceVersion))
		}
		output["items"] = projected
	}
	metadata := output["metadata"].(map[string]any)
	rv, _ := metadata["resourceVersion"].(string)
	b, err := json.Marshal(output)
	return b, rv, err
}
func projectItem(input map[string]any, kind, version string, resourceVersion projection) map[string]any {
	nameField := "metadata.name"
	if kind == "Service" {
		nameField = "Service.metadata.name"
	}
	metaFields := projectionFields{"name": identifierField(nameField), "namespace": identifierField("metadata.namespace"), "resourceVersion": resourceVersion}
	if kind != "Secret" && kind != "ConfigMap" && !strings.HasSuffix(kind, "List") {
		metaFields["labels"] = identifierKeys("label.key")
	}
	if kind == "Pod" {
		metaFields["ownerReferences"] = array(object(projectionFields{"kind": identifierField("reference.kind"), "name": identifierField("reference.name")}))
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
			if keys, ok := identifierKeys("data.key")(input[name]); ok && len(keys.(map[string]any)) > 0 {
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
