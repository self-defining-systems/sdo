package operatorcli_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Build just the leaf's owning shape; arrays use a single item. This independent
// inventory checks each retained identity at the real proxy/storage boundary.
func identityShape(path []string, value any) any {
	if len(path) == 0 {
		return value
	}
	if path[0] == "0" {
		return []any{identityShape(path[1:], value)}
	}
	return map[string]any{path[0]: identityShape(path[1:], value)}
}
func identityAt(value any, path []string) (any, bool) {
	if len(path) == 0 {
		return value, true
	}
	if path[0] == "0" {
		items, ok := value.([]any)
		if !ok || len(items) == 0 {
			return nil, false
		}
		return identityAt(items[0], path[1:])
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	child, ok := obj[path[0]]
	if !ok {
		return nil, false
	}
	return identityAt(child, path[1:])
}

func TestIdentifierGrammarAtPersistenceBoundary(t *testing.T) {
	type leaf struct {
		kind, path, good, bad string
		key                   bool
	}
	var leaves []leaf
	add := func(kind, path, good, bad string) { leaves = append(leaves, leaf{kind, path, good, bad, false}) }
	key := func(kind, path, good, bad string) { leaves = append(leaves, leaf{kind, path, good, bad, true}) }
	for _, kind := range []string{"Pod", "Service", "Deployment", "StatefulSet", "ReplicaSet", "Event", "Endpoints", "EndpointSlice", "HorizontalPodAutoscaler", "PodDisruptionBudget", "NetworkPolicy", "Secret", "ConfigMap"} {
		good := "web.worker"
		bad := "UPPER"
		if kind == "Service" {
			good = "web-service"
			bad = "web.service"
		}
		add(kind, "metadata.name", good, bad)
		add(kind, "metadata.namespace", "app-ns", "app.ns")
		if kind != "Secret" && kind != "ConfigMap" {
			key(kind, "metadata.labels", "example.com/App_Name", "/bad")
		}
	}
	for _, field := range []string{"data", "stringData", "binaryData"} {
		for _, kind := range []string{"Secret", "ConfigMap"} {
			key(kind, field, "Config_Key.json", "../bad")
		}
	}
	for _, kind := range []string{"Pod", "Deployment", "StatefulSet", "ReplicaSet"} {
		prefix := "spec."
		if kind != "Pod" {
			prefix = "spec.template.spec."
			key(kind, "spec.template.metadata.labels", "example.com/App_Name", "/bad")
		}
		add(kind, prefix+"nodeName", "node.example.com", "UPPER")
		add(kind, prefix+"volumes.0.name", "config-vol", "config.vol")
		for _, group := range []string{"containers", "initContainers"} {
			base := prefix + group + ".0."
			add(kind, base+"name", "web-worker", "web.worker")
			add(kind, base+"ports.0.name", "http-web", "UPPER")
			add(kind, base+"volumeMounts.0.name", "config-vol", "config.vol")
			add(kind, base+"env.0.name", "APP_MODE", "APP-MODE")
			add(kind, base+"env.0.valueFrom.resourceFieldRef.containerName", "web-worker", "web.worker")
			for _, ref := range []string{"secretKeyRef", "configMapKeyRef"} {
				add(kind, base+"env.0.valueFrom."+ref+".name", "app.config", "UPPER")
				add(kind, base+"env.0.valueFrom."+ref+".key", "Config_Key.json", "../bad")
			}
			for _, ref := range []string{"secretRef", "configMapRef"} {
				add(kind, base+"envFrom.0."+ref+".name", "app.config", "UPPER")
			}
		}
	}
	for _, group := range []string{"containerStatuses", "initContainerStatuses"} {
		add("Pod", "status."+group+".0.name", "web-worker", "web.worker")
	}
	add("Pod", "metadata.ownerReferences.0.name", "web.worker", "UPPER")
	add("Pod", "metadata.ownerReferences.0.kind", "ReplicaSet", "Bad-Kind")
	add("Pod", "status.conditions.0.type", "example.com/CustomReady", "/bad")
	for _, pair := range [][2]string{{"Event", "involvedObject"}, {"HorizontalPodAutoscaler", "spec.scaleTargetRef"}, {"HorizontalPodAutoscaler", "spec.metrics.0.object.describedObject"}} {
		add(pair[0], pair[1]+".name", "web.worker", "UPPER")
		add(pair[0], pair[1]+".kind", "Deployment", "Bad-Kind")
		add(pair[0], pair[1]+".apiVersion", "apps/v1", "apps/v1/extra")
	}
	add("Event", "involvedObject.namespace", "app-ns", "app.ns")
	for _, source := range []string{"pods", "object", "external"} {
		add("HorizontalPodAutoscaler", "spec.metrics.0."+source+".metric.name", "custom.example.com/queue_depth", "/bad")
	}
	for _, source := range []string{"resource", "containerResource"} {
		add("HorizontalPodAutoscaler", "spec.metrics.0."+source+".name", "example.com/custom_cpu", "/bad")
	}
	add("HorizontalPodAutoscaler", "spec.metrics.0.containerResource.container", "web-worker", "web.worker")
	for _, pair := range [][2]string{{"Service", "spec.selector"}, {"Deployment", "spec.selector.matchLabels"}, {"StatefulSet", "spec.selector.matchLabels"}, {"ReplicaSet", "spec.selector.matchLabels"}, {"PodDisruptionBudget", "spec.selector.matchLabels"}, {"NetworkPolicy", "spec.podSelector.matchLabels"}, {"NetworkPolicy", "spec.ingress.0.from.0.namespaceSelector.matchLabels"}, {"NetworkPolicy", "spec.egress.0.to.0.podSelector.matchLabels"}, {"HorizontalPodAutoscaler", "spec.metrics.0.external.metric.selector.matchLabels"}} {
		key(pair[0], pair[1], "example.com/App_Name", "/bad")
		if pair[1] != "spec.selector" {
			add(pair[0], strings.TrimSuffix(pair[1], "matchLabels")+"matchExpressions.0.key", "example.com/App_Name", "/bad")
		}
	}
	for _, pair := range [][2]string{{"Service", "spec.ports.0.name"}, {"Endpoints", "subsets.0.ports.0.name"}, {"EndpointSlice", "ports.0.name"}} {
		add(pair[0], pair[1], "http-web", "UPPER")
	}
	for _, tc := range leaves {
		for _, list := range []bool{false, true} {
			for _, mode := range []string{"text", "grammar", "accepted"} {
				t.Run(fmt.Sprintf("%s/%s/list=%t/%s", tc.kind, tc.path, list, mode), func(t *testing.T) {
					value := "Authorization: Bearer " + retainedLeafCanary
					if mode == "grammar" {
						value = tc.bad
					}
					if mode == "accepted" {
						value = tc.good
					}
					var input any = value
					if tc.key {
						input = map[string]any{value: "opaque-value"}
					}
					obj := identityShape(strings.Split(tc.path, "."), input).(map[string]any)
					version := "v1"
					switch tc.kind {
					case "Deployment", "StatefulSet", "ReplicaSet":
						version = "apps/v1"
					case "EndpointSlice":
						version = "discovery.k8s.io/v1"
					case "HorizontalPodAutoscaler":
						version = "autoscaling/v2"
					case "PodDisruptionBudget":
						version = "policy/v1"
					case "NetworkPolicy":
						version = "networking.k8s.io/v1"
					}
					metadata, ok := obj["metadata"].(map[string]any)
					if !ok {
						metadata = map[string]any{}
						obj["metadata"] = metadata
					}
					if _, exists := metadata["name"]; !exists {
						metadata["name"] = "p"
					}
					if _, exists := metadata["namespace"]; !exists {
						metadata["namespace"] = "app"
					}
					obj["kind"], obj["apiVersion"] = tc.kind, version
					path := strings.Split(tc.path, ".")
					if tc.key {
						path = append(path, value)
					}
					if list {
						obj = map[string]any{"kind": tc.kind + "List", "apiVersion": version, "items": []any{obj}}
						path = append([]string{"items", "0"}, path...)
					}
					resource := strings.ToLower(tc.kind) + "s"
					switch tc.kind {
					case "Endpoints":
						resource = "endpoints"
					case "NetworkPolicy":
						resource = "networkpolicies"
					}
					requestPath := "/api/" + version + "/namespaces/app/" + resource
					if strings.Contains(version, "/") {
						requestPath = "/apis/" + version + "/namespaces/app/" + resource
					}
					got := recordRetainedLeaf(t, requestPath, "GET", leafJSON(t, obj), "application/json", "")
					var projected any
					if err := json.Unmarshal(got, &projected); err != nil {
						t.Fatal(err)
					}
					retained, exists := identityAt(projected, path)
					if mode == "accepted" {
						want := value
						if tc.key {
							want = ""
						}
						if !exists || retained != want {
							t.Fatalf("accepted identifier lost: %s", got)
						}
					} else if exists {
						t.Fatalf("invalid identifier retained: %s", got)
					}
				})
			}
		}
	}
}

func TestWorkloadConditionTypeVocabulary(t *testing.T) {
	for _, kind := range []string{"Deployment", "StatefulSet", "ReplicaSet"} {
		for _, list := range []bool{false, true} {
			for _, value := range []string{"Authorization: Bearer " + retainedLeafCanary, retainedLeafCanary, "Available", "Progressing", "ReplicaFailure"} {
				t.Run(fmt.Sprintf("%s/list=%t/%s", kind, list, value), func(t *testing.T) {
					obj := map[string]any{"kind": kind, "apiVersion": "apps/v1", "metadata": map[string]any{"name": "p", "namespace": "app"}, "status": map[string]any{"conditions": []any{map[string]any{"type": value, "status": "False", "reason": "FailedCreate"}}}}
					path := []string{"status", "conditions", "0", "type"}
					if list {
						obj = map[string]any{"kind": kind + "List", "apiVersion": "apps/v1", "items": []any{obj}}
						path = append([]string{"items", "0"}, path...)
					}
					got := recordRetainedLeaf(t, "/apis/apps/v1/namespaces/app/"+strings.ToLower(kind)+"s", "GET", leafJSON(t, obj), "application/json", "")
					var projected any
					if err := json.Unmarshal(got, &projected); err != nil {
						t.Fatal(err)
					}
					want := value
					if strings.Contains(value, retainedLeafCanary) {
						want = "redacted"
					}
					if retained, ok := identityAt(projected, path); !ok || retained != want {
						t.Fatalf("condition type policy: %s", got)
					}
				})
			}
		}
	}
}

func TestHPACrossVersionReferenceHasNoNamespace(t *testing.T) {
	for _, path := range []string{"spec.scaleTargetRef", "spec.metrics.0.object.describedObject"} {
		obj := identityShape(strings.Split(path, "."), map[string]any{"name": "web", "kind": "Deployment", "apiVersion": "apps/v1", "namespace": retainedLeafCanary}).(map[string]any)
		obj["kind"], obj["apiVersion"] = "HorizontalPodAutoscaler", "autoscaling/v2"
		got := recordRetainedLeaf(t, "/apis/autoscaling/v2/namespaces/app/horizontalpodautoscalers/hpa", "GET", leafJSON(t, obj), "application/json", "")
		var projected any
		if err := json.Unmarshal(got, &projected); err != nil {
			t.Fatal(err)
		}
		if _, ok := identityAt(projected, strings.Split(path+".namespace", ".")); ok {
			t.Fatal("invented namespace retained")
		}
	}
}
