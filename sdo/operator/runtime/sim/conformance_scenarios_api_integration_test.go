//go:build integration

package sim_test

import "encoding/json"

const applyType = "application/apply-patch+yaml"

func applyAs(name, path, manager string, force bool, body any, extra ...string) step {
	q := map[string]string{"fieldManager": manager}
	if force {
		q["force"] = "true"
	}
	for i := 0; i+1 < len(extra); i += 2 {
		q[extra[i]] = extra[i+1]
	}
	return step{Name: name, Method: "PATCH", Path: path, ContentType: applyType, Query: q, Body: body}
}

func patchStep(name, path, ctype string, body any) step {
	return step{Name: name, Method: "PATCH", Path: path, ContentType: ctype, Body: body}
}

const (
	mergeType     = "application/merge-patch+json"
	strategicType = "application/strategic-merge-patch+json"
	jsonPatchType = "application/json-patch+json"
)

func cm(name string, data obj, extra ...obj) obj {
	return obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta(name, extra...), "data": data}
}

func appliedService(name string, port int) obj {
	return obj{"apiVersion": "v1", "kind": "Service", "metadata": obj{"name": name, "namespace": "{ns}"}, "spec": obj{"selector": obj{"app": name}, "ports": []any{obj{"port": port}}}}
}

func appliedDeployment(name string, containers ...any) obj {
	return obj{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": obj{"name": name, "namespace": "{ns}"}, "spec": obj{
		"selector": obj{"matchLabels": obj{"app": name}},
		"template": obj{"metadata": obj{"labels": obj{"app": name}}, "spec": obj{"containers": containers}}}}
}

func rvOf(name string) func(*run) any {
	return func(r *run) any { return r.rv(name) }
}

// bodyFn builds a body from earlier results.
func bodyFn(f func(*run) any) func(*run) any { return f }

var apiScenarios = []scenario{
	{Name: "ssa/create-then-noop-apply", Monotonic: true, Steps: []step{
		applyAs("create", svcPath+"/web", "deployer", true, appliedService("web", 80)),
		applyAs("again", svcPath+"/web", "deployer", true, appliedService("web", 80)),
		applyAs("change", svcPath+"/web", "deployer", true, appliedService("web", 8080)),
		get("get", svcPath+"/web"),
	}},
	{Name: "ssa/conflict-and-force", Steps: []step{
		applyAs("a", cmPath+"/c", "a", false, cm("c", obj{"k": "1"})),
		applyAs("b-conflicts", cmPath+"/c", "b", false, cm("c", obj{"k": "2"})),
		applyAs("b-same-value", cmPath+"/c", "b", false, cm("c", obj{"k": "1"})),
		applyAs("b-forces", cmPath+"/c", "b", true, cm("c", obj{"k": "2"})),
		applyAs("a-now-conflicts", cmPath+"/c", "a", false, cm("c", obj{"k": "1"})),
		get("get", cmPath+"/c"),
	}},
	{Name: "ssa/field-removal-and-disjoint-owners", Steps: []step{
		applyAs("a", cmPath+"/c", "a", true, cm("c", obj{"x": "1", "y": "2"})),
		applyAs("b", cmPath+"/c", "b", true, cm("c", obj{"z": "3"})),
		applyAs("a-drops-y", cmPath+"/c", "a", true, cm("c", obj{"x": "1"})),
		get("get", cmPath+"/c"),
	}},
	{Name: "ssa/list-map-merge-by-key", Steps: []step{
		applyAs("a", depPath+"/web", "a", true, appliedDeployment("web", obj{"name": "app", "image": "nginx:1", "env": []any{obj{"name": "A", "value": "1"}}})),
		applyAs("b-adds-sidecar-and-env", depPath+"/web", "b", true, appliedDeployment("web",
			obj{"name": "app", "env": []any{obj{"name": "B", "value": "2"}}},
			obj{"name": "side", "image": "busybox:1"})),
		applyAs("a-changes-image", depPath+"/web", "a", true, appliedDeployment("web", obj{"name": "app", "image": "nginx:2", "env": []any{obj{"name": "A", "value": "1"}}})),
		get("get", depPath+"/web"),
	}},
	{Name: "ssa/update-owned-field-taken-by-apply", Steps: []step{
		create("create", depPath, deployment("web", obj{"name": "app", "image": "nginx:1"})),
		applyAs("deployer-forces", depPath+"/web", "sdo-deployer", true, appliedDeployment("web", obj{"name": "app", "image": "nginx:2"})),
		applyAs("deployer-without-force", depPath+"/web", "sdo-deployer", false, appliedDeployment("web", obj{"name": "app", "image": "nginx:3"})),
	}},
	{Name: "ssa/dry-run-apply-does-not-persist", Steps: []step{
		applyAs("create", svcPath+"/web", "deployer", true, appliedService("web", 80)),
		applyAs("dry", svcPath+"/web", "deployer", true, appliedService("web", 9090), "dryRun", "All"),
		get("get", svcPath+"/web"),
		applyAs("dry-absent", svcPath+"/other", "deployer", true, appliedService("other", 81), "dryRun", "All"),
		get("absent", svcPath+"/other"),
	}},
	{Name: "ssa/dry-run-create-renders-defaults", Steps: []step{
		create("create", svcPath, service("web", obj{"selector": obj{"app": "web"}, "ports": []any{obj{"port": 80}}})),
		step{Name: "probe", Method: "POST", Path: svcPath, Query: map[string]string{"dryRun": "All"}, Body: service("web-sdo-default-render", obj{"selector": obj{"app": "web"}, "ports": []any{obj{"port": 80}}})},
		step{Name: "exists", Method: "POST", Path: svcPath, Query: map[string]string{"dryRun": "All"}, Body: service("web", obj{"ports": []any{obj{"port": 80}}})},
		get("probe-gone", svcPath+"/web-sdo-default-render"),
	}},
	{Name: "ssa/acceptance-record-keeps-other-fields", Steps: []step{
		applyAs("deploy", depPath+"/web", "sdo-deployer", true, appliedDeployment("web", obj{"name": "app", "image": "nginx:1"})),
		step{Name: "record", Method: "PATCH", Path: depPath + "/web", ContentType: applyType, Query: map[string]string{"fieldManager": "sdo-acceptance", "force": "true"},
			Fn: func(r *run) any {
				return obj{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": obj{"name": "web", "namespace": "{ns}", "uid": r.uid("deploy"), "annotations": obj{"sdo.dev/accepted": "paths"}}}
			}},
		applyAs("deploy-again", depPath+"/web", "sdo-deployer", true, appliedDeployment("web", obj{"name": "app", "image": "nginx:2"})),
		get("get", depPath+"/web"),
	}},
	{Name: "ssa/apply-validation", Steps: []step{
		step{Name: "no-manager", Method: "PATCH", Path: cmPath + "/c", ContentType: applyType, Body: cm("c", obj{"k": "v"})},
		applyAs("wrong-kind", cmPath+"/c", "a", true, obj{"apiVersion": "v1", "kind": "Service", "metadata": obj{"name": "c"}}),
		applyAs("wrong-name", cmPath+"/c", "a", true, cm("other", obj{"k": "v"})),
	}},

	{Name: "patch/merge-patch-semantics", Steps: []step{
		create("create", depPath, deployment("web", obj{"name": "app", "image": "nginx:1", "ports": []any{obj{"containerPort": 80}}}, obj{"replicas": 2})),
		patchStep("annotate", depPath+"/web", mergeType, obj{"metadata": obj{"annotations": obj{"a": "1", "b": "2"}}}),
		patchStep("null-deletes", depPath+"/web", mergeType, obj{"metadata": obj{"annotations": obj{"a": nil}}}),
		patchStep("list-replaces", depPath+"/web", mergeType, obj{"spec": obj{"template": obj{"spec": obj{"containers": []any{obj{"name": "app", "image": "nginx:2"}}}}}}),
		patchStep("replicas", depPath+"/web", mergeType, obj{"spec": obj{"replicas": 5}}),
	}},
	{Name: "patch/merge-noop-keeps-version", Monotonic: true, Steps: []step{
		create("create", cmPath, cm("c", obj{"k": "v"})),
		patchStep("same", cmPath+"/c", mergeType, obj{"data": obj{"k": "v"}}),
		patchStep("different", cmPath+"/c", mergeType, obj{"data": obj{"k": "w"}}),
	}},
	{Name: "patch/strategic-merge", Steps: []step{
		create("create", depPath, deployment("web", obj{"name": "app", "image": "nginx:1", "env": []any{obj{"name": "A", "value": "1"}}})),
		patchStep("set-image", depPath+"/web", strategicType, obj{"spec": obj{"template": obj{"spec": obj{"containers": []any{obj{"name": "app", "image": "nginx:2"}}}}}}),
		patchStep("add-container", depPath+"/web", strategicType, obj{"spec": obj{"template": obj{"spec": obj{"containers": []any{obj{"name": "side", "image": "busybox"}}}}}}),
		patchStep("delete-container", depPath+"/web", strategicType, obj{"spec": obj{"template": obj{"spec": obj{"containers": []any{obj{"name": "side", "$patch": "delete"}}}}}}),
		patchStep("add-env", depPath+"/web", strategicType, obj{"spec": obj{"template": obj{"spec": obj{"containers": []any{obj{"name": "app", "env": []any{obj{"name": "B", "value": "2"}}}}}}}}),
	}},
	{Name: "patch/json-patch", Steps: []step{
		create("create", svcPath, service("web", obj{"selector": obj{"app": "web"}, "ports": []any{obj{"port": 80}}})),
		patchStep("replace", svcPath+"/web", jsonPatchType, []any{obj{"op": "replace", "path": "/spec/ports/0/port", "value": 8080}}),
		patchStep("add-label", svcPath+"/web", jsonPatchType, []any{obj{"op": "add", "path": "/metadata/labels", "value": obj{"k": "v"}}}),
		patchStep("bad-path", svcPath+"/web", jsonPatchType, []any{obj{"op": "remove", "path": "/spec/nothing"}}),
	}},
	{Name: "patch/preconditions-and-missing", Steps: []step{
		create("create", cmPath, cm("c", obj{"k": "v"})),
		create("other", cmPath, cm("other", obj{"k": "v"})),
		patchStep("stale-rv", cmPath+"/c", mergeType, obj{"metadata": obj{"resourceVersion": "7"}, "data": obj{"k": "w"}}),
		step{Name: "current-rv", Method: "PATCH", Path: cmPath + "/c", ContentType: mergeType, Fn: func(r *run) any {
			return obj{"metadata": obj{"resourceVersion": r.rv("create")}, "data": obj{"k": "w"}}
		}},
		step{Name: "wrong-uid", Method: "PATCH", Path: cmPath + "/c", ContentType: mergeType, Body: obj{"metadata": obj{"uid": "not-this"}, "data": obj{"k": "x"}}},
		patchStep("missing", cmPath+"/nothing", mergeType, obj{"data": obj{"k": "x"}}),
		patchStep("unsupported-type", cmPath+"/c", "text/plain", "x"),
	}},
	{Name: "update/put-versions", Steps: []step{
		create("create", cmPath, cm("c", obj{"k": "v"})),
		step{Name: "put-unconditional", Method: "PUT", Path: cmPath + "/c", Body: cm("c", obj{"k": "w"})},
		step{Name: "put-stale", Method: "PUT", Path: cmPath + "/c", Body: cm("c", obj{"k": "x"}, obj{"resourceVersion": "1"})},
		step{Name: "put-missing", Method: "PUT", Path: cmPath + "/nothing", Body: cm("nothing", obj{"k": "x"})},
		create("duplicate", cmPath, cm("c", obj{"k": "v"})),
		create("wrong-namespace", cmPath, obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": obj{"name": "z", "namespace": "elsewhere"}}),
	}},

	{Name: "status/generation-and-status-subresource", Monotonic: true, Steps: []step{
		create("create", depPath, deployment("web", obj{"name": "app", "image": "nginx:1"})),
		patchStep("label", depPath+"/web", mergeType, obj{"metadata": obj{"labels": obj{"k": "v"}}}),
		patchStep("replicas", depPath+"/web", mergeType, obj{"spec": obj{"replicas": 3}}),
		patchStep("status-ignored-on-main", depPath+"/web", mergeType, obj{"status": obj{"readyReplicas": 3}}),
		patchStep("status-write", depPath+"/web/status", mergeType, obj{"status": obj{"replicas": 3, "readyReplicas": 3, "observedGeneration": 2}}),
		patchStep("spec-ignored-on-status", depPath+"/web/status", mergeType, obj{"spec": obj{"replicas": 9}}),
		get("get-status", depPath+"/web/status"),
		get("get", depPath+"/web"),
	}},
	{Name: "status/pod-status-and-no-status-kinds", Steps: []step{
		create("pod", podPath, pod("p", obj{"name": "app", "image": "nginx:1"})),
		patchStep("pod-status", podPath+"/p/status", mergeType, obj{"status": obj{"phase": "Running"}}),
		patchStep("pod-label", podPath+"/p", mergeType, obj{"metadata": obj{"labels": obj{"k": "v"}}}),
		create("cm", cmPath, cm("c", obj{"k": "v"})),
		get("cm-status", cmPath+"/c/status"),
	}},

	{Name: "delete/basic-and-preconditions", Steps: []step{
		create("create", cmPath, cm("c", obj{"k": "v"})),
		step{Name: "wrong-uid", Method: "DELETE", Path: cmPath + "/c", Body: obj{"preconditions": obj{"uid": "not-this"}}},
		step{Name: "wrong-rv", Method: "DELETE", Path: cmPath + "/c", Body: obj{"preconditions": obj{"resourceVersion": "7"}}},
		step{Name: "dry-run", Method: "DELETE", Path: cmPath + "/c", Query: map[string]string{"dryRun": "All"}},
		get("still-there", cmPath+"/c"),
		step{Name: "delete", Method: "DELETE", Path: cmPath + "/c", Fn: func(r *run) any {
			return obj{"preconditions": obj{"uid": r.uid("create"), "resourceVersion": r.rv("create")}}
		}},
		get("gone", cmPath+"/c"),
		step{Name: "delete-missing", Method: "DELETE", Path: cmPath + "/c"},
	}},
	{Name: "delete/finalizers", Monotonic: true, Steps: []step{
		create("create", cmPath, cm("c", obj{"k": "v"}, obj{"finalizers": []any{"example.com/hold"}})),
		step{Name: "delete", Method: "DELETE", Path: cmPath + "/c"},
		step{Name: "delete-again", Method: "DELETE", Path: cmPath + "/c"},
		get("terminating", cmPath+"/c"),
		patchStep("add-finalizer-while-terminating", cmPath+"/c", mergeType, obj{"metadata": obj{"finalizers": []any{"example.com/hold", "example.com/other"}}}),
		patchStep("release-one", cmPath+"/c", mergeType, obj{"metadata": obj{"finalizers": []any{"example.com/other"}}}),
		patchStep("release-last", cmPath+"/c", mergeType, obj{"metadata": obj{"finalizers": nil}}),
		get("gone", cmPath+"/c"),
	}},
	{Name: "delete/pod-and-recreate-gets-new-identity", Steps: []step{
		create("create", podPath, pod("p", obj{"name": "app", "image": "nginx:1"})),
		step{Name: "delete", Method: "DELETE", Path: podPath + "/p"},
		create("recreate", podPath, pod("p", obj{"name": "app", "image": "nginx:1"})),
		step{Name: "delete-old-uid", Method: "DELETE", Path: podPath + "/p", Fn: func(r *run) any { return obj{"preconditions": obj{"uid": r.uid("create")}} }},
	}},

	{Name: "list/order-selectors-and-versions", Steps: []step{
		create("b", cmPath, cm("b", obj{"k": "1"}, obj{"labels": obj{"tier": "x"}})),
		create("a", cmPath, cm("a", obj{"k": "2"}, obj{"labels": obj{"tier": "y"}})),
		create("c", cmPath, cm("c", obj{"k": "3"}, obj{"labels": obj{"tier": "x"}})),
		get("all", cmPath),
		step{Name: "label", Method: "GET", Path: cmPath, Query: map[string]string{"labelSelector": "tier=x"}},
		step{Name: "label-notin", Method: "GET", Path: cmPath, Query: map[string]string{"labelSelector": "tier notin (x)"}},
		step{Name: "field", Method: "GET", Path: cmPath, Query: map[string]string{"fieldSelector": "metadata.name=c"}},
		step{Name: "bad-selector", Method: "GET", Path: cmPath, Query: map[string]string{"labelSelector": "!!"}},
		get("services-empty", svcPath),
	}},
	{Name: "list/pods-by-phase", Steps: []step{
		create("p1", podPath, pod("p1", obj{"name": "app", "image": "nginx:1"})),
		create("p2", podPath, pod("p2", obj{"name": "app", "image": "nginx:1"})),
		patchStep("run-p2", podPath+"/p2/status", mergeType, obj{"status": obj{"phase": "Running"}}),
		step{Name: "pending", Method: "GET", Path: podPath, Query: map[string]string{"fieldSelector": "status.phase=Pending"}},
		step{Name: "running", Method: "GET", Path: podPath, Query: map[string]string{"fieldSelector": "status.phase=Running"}},
	}},

	{Name: "watch/replay-after-version", Steps: []step{
		create("a", cmPath, cm("a", obj{"k": "1"})),
		create("b", cmPath, cm("b", obj{"k": "2"})),
		patchStep("patch-a", cmPath+"/a", mergeType, obj{"data": obj{"k": "3"}}),
		step{Name: "delete-b", Method: "DELETE", Path: cmPath + "/b"},
		create("pod", podPath, pod("p", obj{"name": "app", "image": "nginx:1"})),
		step{Name: "watch-cms", Method: "WATCH", Path: cmPath, Query: map[string]string{"resourceVersion": "@a"}},
		step{Name: "watch-from-start", Method: "WATCH", Path: cmPath},
		step{Name: "watch-selected", Method: "WATCH", Path: cmPath, Query: map[string]string{"resourceVersion": "@a", "fieldSelector": "metadata.name=a"}},
	}},
	{Name: "watch/status-writes-and-finalizers", Steps: []step{
		create("dep", depPath, deployment("web", obj{"name": "app", "image": "nginx:1"})),
		patchStep("status", depPath+"/web/status", mergeType, obj{"status": obj{"replicas": 1}}),
		create("held", cmPath, cm("held", obj{"k": "v"}, obj{"finalizers": []any{"example.com/hold"}})),
		step{Name: "delete-held", Method: "DELETE", Path: cmPath + "/held"},
		patchStep("release", cmPath+"/held", mergeType, obj{"metadata": obj{"finalizers": nil}}),
		step{Name: "watch-deployments", Method: "WATCH", Path: depPath},
		step{Name: "watch-configmaps", Method: "WATCH", Path: cmPath, Query: map[string]string{"resourceVersion": "@dep"}},
	}},
	{Name: "lease/acquire-renew-release", Steps: []step{
		create("acquire", leasePath, lease("leader", "controller-a", 3, "2026-11-03T10:00:00.000000Z", nil)),
		get("get", leasePath+"/leader"),
		step{Name: "renew", Method: "PUT", Path: leasePath + "/leader", Fn: func(r *run) any {
			return lease("leader", "controller-a", 3, "2026-11-03T10:00:00.000000Z", obj{"resourceVersion": r.rv("acquire"), "renewTime": "2026-11-03T10:00:01.500000Z"})
		}},
		step{Name: "renew-stale", Method: "PUT", Path: leasePath + "/leader", Fn: func(r *run) any {
			return lease("leader", "controller-a", 3, "2026-11-03T10:00:00.000000Z", obj{"resourceVersion": r.rv("acquire"), "renewTime": "2026-11-03T10:00:02.000000Z"})
		}},
		step{Name: "release", Method: "PUT", Path: leasePath + "/leader", Fn: func(r *run) any {
			return lease("leader", "", 1, "2026-11-03T10:00:00.000000Z", obj{"resourceVersion": r.rv("renew")})
		}},
		create("create-again", leasePath, lease("leader", "controller-b", 3, "2026-11-03T10:00:09.000000Z", nil)),
		get("after-release", leasePath+"/leader"),
	}},
	{Name: "lease/takeover-and-watch", Steps: []step{
		create("acquire", leasePath, lease("leader", "controller-a", 3, "2026-11-03T10:00:00.000000Z", nil)),
		get("list-before", leasePath),
		step{Name: "takeover", Method: "PUT", Path: leasePath + "/leader", Fn: func(r *run) any {
			return lease("leader", "controller-b", 3, "2026-11-03T10:00:04.000000Z", obj{"resourceVersion": r.rv("acquire"), "leaseTransitions": 1})
		}},
		step{Name: "loser-write", Method: "PUT", Path: leasePath + "/leader", Fn: func(r *run) any {
			return lease("leader", "controller-a", 3, "2026-11-03T10:00:05.000000Z", obj{"resourceVersion": r.rv("acquire")})
		}},
		step{Name: "delete", Method: "DELETE", Path: leasePath + "/leader"},
		step{Name: "watch-from-list", Method: "WATCH", Path: leasePath, Query: map[string]string{"resourceVersion": "@list-before"}},
	}},
	{Name: "watch/informer-list-then-watch", Steps: []step{
		create("a", cmPath, cm("a", obj{"k": "1"})),
		get("list", cmPath),
		create("b", cmPath, cm("b", obj{"k": "2"})),
		patchStep("patch-a", cmPath+"/a", mergeType, obj{"data": obj{"k": "3"}}),
		step{Name: "watch-after-list", Method: "WATCH", Path: cmPath, Query: map[string]string{"resourceVersion": "@list"}},
		step{Name: "watch-bookmarks", Method: "WATCH", Path: cmPath, Query: map[string]string{"resourceVersion": "@list", "allowWatchBookmarks": "true", "timeoutSeconds": "1"}},
	}},
	{Name: "scale/autoscaler-owns-replicas", Steps: []step{
		create("create", depPath, deployment("web", obj{"name": "app", "image": "nginx:1"}, obj{"replicas": 2})),
		step{Name: "get-scale", Method: "GET", Path: depPath + "/web/scale"},
		step{Name: "scale", Method: "PATCH", Path: depPath + "/web/scale", ContentType: mergeType, Query: map[string]string{"fieldManager": "hpa"}, Body: obj{"spec": obj{"replicas": 4}}},
		get("get", depPath+"/web"),
	}},
	{Name: "cluster-scoped/node", Steps: []step{
		step{Name: "create", Method: "POST", Path: "/api/v1/nodes", Body: obj{"apiVersion": "v1", "kind": "Node", "metadata": obj{"name": "n-{ns}"}}},
		step{Name: "get", Method: "GET", Path: "/api/v1/nodes/n-{ns}"},
		step{Name: "delete", Method: "DELETE", Path: "/api/v1/nodes/n-{ns}"},
	}},
	{Name: "metadata-only/secret-reads", Steps: []step{
		create("create", "/api/v1/namespaces/{ns}/secrets", obj{"apiVersion": "v1", "kind": "Secret", "metadata": meta("s"), "stringData": obj{"token": "hunter2"}}),
		step{Name: "list-meta", Method: "GET", Path: "/api/v1/namespaces/{ns}/secrets", Accept: "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json"},
		step{Name: "get-meta", Method: "GET", Path: "/api/v1/namespaces/{ns}/secrets/s", Accept: "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json"},
	}},
	{Name: "bounds/missing-namespace", Accepted: map[string]string{
		"create: *": "namespaces are not objects in the simulation, so a create in a missing namespace succeeds; tests build worlds without Namespace objects",
	}, Steps: []step{
		step{Name: "create", Method: "POST", Path: "/api/v1/namespaces/does-not-exist/configmaps", Body: obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": obj{"name": "c", "namespace": "does-not-exist"}}},
	}},
	{Name: "bounds/validation", Accepted: map[string]string{
		"bad-name: *":           "no object validation: names are not checked against DNS-1123 rules",
		"selector-immutable: *": "no immutability validation: a Deployment selector change is accepted",
		"no-selector: *":        "no required-field validation: a Deployment without a selector is accepted",
		"no-image: *":           "no required-field validation: a container without an image is accepted",
	}, Steps: []step{
		create("deployment", depPath, deployment("web", obj{"name": "app", "image": "nginx:1"})),
		create("bad-name", cmPath, cm("Bad_Name", obj{})),
		patchStep("selector-immutable", depPath+"/web", mergeType, obj{"spec": obj{"selector": obj{"matchLabels": obj{"app": "other"}}, "template": obj{"metadata": obj{"labels": obj{"app": "other"}}}}}),
		create("no-selector", depPath, obj{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": meta("bare"), "spec": obj{"template": obj{"spec": obj{"containers": []any{obj{"name": "a", "image": "x"}}}}}}),
		create("no-image", podPath, obj{"apiVersion": "v1", "kind": "Pod", "metadata": meta("noimg"), "spec": obj{"containers": []any{obj{"name": "a"}}}}),
	}},
	{Name: "discovery/served-resources", Project: projectDiscovery, Accepted: map[string]string{
		"apis: .groups[].versions[].groupVersion": "the real server also lists autoscaling/v1 (legacy HPA); the simulation serves only autoscaling/v2",
		"apis: .groups[].versions[].version":      "the real server also lists autoscaling/v1 (legacy HPA); the simulation serves only autoscaling/v2",
	}, Steps: []step{
		get("api", "/api"),
		get("apis", "/apis"),
		get("core", "/api/v1"),
		get("apps", "/apis/apps/v1"),
		get("batch", "/apis/batch/v1"),
		get("networking", "/apis/networking.k8s.io/v1"),
		get("autoscaling", "/apis/autoscaling/v2"),
		get("policy", "/apis/policy/v1"),
		get("discovery", "/apis/discovery.k8s.io/v1"),
		get("unknown-version", "/apis/apps/v9"),
	}},
}

// projectDiscovery keeps what the simulation claims to serve: the resources it
// lists and their status subresource, the groups it knows, and no per-server
// hashes or addresses.
func projectDiscovery(step string, object any) any {
	m, ok := object.(map[string]any)
	if !ok {
		return object
	}
	served := map[string]bool{}
	for _, name := range []string{"configmaps", "endpoints", "events", "nodes", "persistentvolumeclaims", "pods", "secrets", "serviceaccounts", "services", "daemonsets", "deployments", "replicasets", "statefulsets", "horizontalpodautoscalers", "cronjobs", "jobs", "endpointslices", "ingresses", "networkpolicies", "poddisruptionbudgets"} {
		served[name], served[name+"/status"], served[name+"/scale"] = true, true, true
	}
	delete(m, "serverAddressByClientCIDRs")
	if resources, ok := m["resources"].([]any); ok {
		var keep []any
		for _, r := range resources {
			rm := r.(map[string]any)
			delete(rm, "storageVersionHash")
			if served[rm["name"].(string)] {
				keep = append(keep, rm)
			}
		}
		m["resources"] = keep
	}
	if groups, ok := m["groups"].([]any); ok {
		var keep []any
		known := map[string]bool{"apps": true, "autoscaling": true, "batch": true, "discovery.k8s.io": true, "networking.k8s.io": true, "policy": true}
		for _, g := range groups {
			if known[g.(map[string]any)["name"].(string)] {
				keep = append(keep, g)
			}
		}
		m["groups"] = keep
	}
	_ = json.Marshal
	return m
}

const leasePath = "/apis/coordination.k8s.io/v1/namespaces/{ns}/leases"

// lease builds a Lease the way the controller's leader election writes one.
func lease(name, holder string, seconds int, acquired string, extra obj) obj {
	spec := obj{"leaseDurationSeconds": seconds, "acquireTime": acquired, "renewTime": acquired}
	if holder != "" {
		spec["holderIdentity"] = holder
	}
	metadata := meta(name)
	for k, v := range extra {
		if k == "resourceVersion" {
			metadata[k] = v
		} else {
			spec[k] = v
		}
	}
	return obj{"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": metadata, "spec": spec}
}
