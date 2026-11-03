//go:build integration

package sim_test

import (
	"fmt"
	"testing"
)

type obj = map[string]any

const (
	svcPath = "/api/v1/namespaces/{ns}/services"
	podPath = "/api/v1/namespaces/{ns}/pods"
	cmPath  = "/api/v1/namespaces/{ns}/configmaps"
	depPath = "/apis/apps/v1/namespaces/{ns}/deployments"
	stsPath = "/apis/apps/v1/namespaces/{ns}/statefulsets"
	jobPath = "/apis/batch/v1/namespaces/{ns}/jobs"
)

func meta(name string, extra ...obj) obj {
	m := obj{"name": name, "namespace": "{ns}"}
	for _, e := range extra {
		for k, v := range e {
			m[k] = v
		}
	}
	return m
}

func service(name string, spec obj) obj {
	return obj{"apiVersion": "v1", "kind": "Service", "metadata": meta(name), "spec": spec}
}

func deployment(name string, container obj, spec ...obj) obj {
	d := obj{"selector": obj{"matchLabels": obj{"app": name}}, "template": obj{"metadata": obj{"labels": obj{"app": name}}, "spec": obj{"containers": []any{container}}}}
	for _, s := range spec {
		for k, v := range s {
			d[k] = v
		}
	}
	return obj{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": meta(name), "spec": d}
}

func pod(name string, container obj) obj {
	return obj{"apiVersion": "v1", "kind": "Pod", "metadata": meta(name, obj{"labels": obj{"app": name}}), "spec": obj{"containers": []any{container}}}
}

func create(name, path string, body any) step {
	return step{Name: name, Method: "POST", Path: path, Body: body}
}

func get(name, path string) step { return step{Name: name, Method: "GET", Path: path} }

var scenarios = []scenario{
	{Name: "defaults/service-clusterip", Steps: []step{
		create("create", svcPath, service("web", obj{"selector": obj{"app": "web"}, "ports": []any{obj{"port": 80}}})),
		get("get", svcPath+"/web"),
	}},
	{Name: "defaults/service-headless-and-nodeport", Steps: []step{
		create("headless", svcPath, service("h", obj{"clusterIP": "None", "selector": obj{"app": "h"}, "ports": []any{obj{"port": 80, "targetPort": "http"}}})),
		create("nodeport", svcPath, service("np", obj{"type": "NodePort", "ports": []any{obj{"port": 80, "name": "http"}}})),
		create("external", svcPath, service("ext", obj{"type": "ExternalName", "externalName": "example.com"})),
	}},
	{Name: "defaults/deployment", Steps: []step{
		create("create", depPath, deployment("web", obj{"name": "app", "image": "nginx", "ports": []any{obj{"containerPort": 80}}})),
	}},
	{Name: "defaults/deployment-probes-env-volumes", Steps: []step{
		create("create", depPath, deployment("web", obj{
			"name": "app", "image": "nginx:1.25", "imagePullPolicy": "Always",
			"readinessProbe": obj{"httpGet": obj{"path": "/", "port": 80}},
			"livenessProbe":  obj{"tcpSocket": obj{"port": 80}},
			"env":            []any{obj{"name": "POD", "valueFrom": obj{"fieldRef": obj{"fieldPath": "metadata.name"}}}},
			"resources":      obj{"limits": obj{"cpu": "1000m", "memory": "0.5Gi"}},
			"volumeMounts":   []any{obj{"name": "cfg", "mountPath": "/etc/cfg"}},
		}, obj{"strategy": obj{"type": "RollingUpdate"}}, obj{"replicas": 2}, obj{"template": obj{"metadata": obj{"labels": obj{"app": "web"}}, "spec": obj{
			"containers": []any{obj{"name": "app", "image": "nginx:1.25", "imagePullPolicy": "Always",
				"readinessProbe": obj{"httpGet": obj{"path": "/", "port": 80}},
				"livenessProbe":  obj{"tcpSocket": obj{"port": 80}},
				"env":            []any{obj{"name": "POD", "valueFrom": obj{"fieldRef": obj{"fieldPath": "metadata.name"}}}},
				"resources":      obj{"limits": obj{"cpu": "1000m", "memory": "0.5Gi"}},
				"volumeMounts":   []any{obj{"name": "cfg", "mountPath": "/etc/cfg"}}}},
			"volumes": []any{obj{"name": "cfg", "configMap": obj{"name": "c"}}}}}})),
	}},
	{Name: "defaults/pod", Steps: []step{
		create("create", podPath, pod("p", obj{"name": "app", "image": "nginx:latest"})),
		get("get", podPath+"/p"),
	}},
	{Name: "defaults/configmap", Steps: []step{
		create("create", cmPath, obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta("c"), "data": obj{"k": "v"}}),
	}},
	{Name: "defaults/statefulset", Steps: []step{
		create("create", stsPath, obj{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": meta("db"), "spec": obj{
			"serviceName": "db", "selector": obj{"matchLabels": obj{"app": "db"}},
			"template": obj{"metadata": obj{"labels": obj{"app": "db"}}, "spec": obj{"containers": []any{obj{"name": "db", "image": "postgres"}}}}}}),
	}},
	{Name: "defaults/job", Steps: []step{
		create("create", jobPath, obj{"apiVersion": "batch/v1", "kind": "Job", "metadata": meta("j"), "spec": obj{
			"template": obj{"spec": obj{"restartPolicy": "Never", "containers": []any{obj{"name": "j", "image": "busybox"}}}}}}),
	}},
}

func TestKubeConformance(t *testing.T) {
	real := startEnvtest(t)
	for _, sc := range append(append([]scenario{}, scenarios...), apiScenarios...) {
		t.Run(sc.Name, func(t *testing.T) {
			runScenario(t, real, newSim(t), sc)
		})
	}
}

var _ = fmt.Sprint
