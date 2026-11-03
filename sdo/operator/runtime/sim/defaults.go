package sim

import (
	"encoding/base64"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
)

// canonicalize round-trips an object through its typed API form, as the server's
// codec does on every write: quantities become canonical ("1000m" is stored as
// "1", "0.5Gi" as "512Mi"), unknown fields are dropped, and always-present empty
// structs (resources, securityContext, status) appear. Null leaves are removed.
func canonicalize(object *unstructured.Unstructured) error {
	typed, err := scheme.Scheme.New(object.GroupVersionKind())
	if err != nil {
		return nil
	}
	managed := object.GetManagedFields()
	object.SetManagedFields(nil)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err != nil {
		return fmt.Errorf("decode %s: %w", object.GetKind(), err)
	}
	out, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed)
	if err != nil {
		return err
	}
	pruneNulls(out)
	out["apiVersion"], out["kind"] = object.GetAPIVersion(), object.GetKind()
	object.Object = out
	object.SetManagedFields(managed)
	return nil
}

func pruneNulls(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			if e == nil {
				delete(x, k)
				continue
			}
			pruneNulls(e)
		}
	case []any:
		for _, e := range x {
			pruneNulls(e)
		}
	}
}

func setDefault(m map[string]any, key string, value any) {
	if _, ok := m[key]; !ok {
		m[key] = value
	}
}

func child(m map[string]any, key string) map[string]any {
	c, _ := m[key].(map[string]any)
	return c
}

func children(m map[string]any, key string) []map[string]any {
	var out []map[string]any
	items, _ := m[key].([]any)
	for _, e := range items {
		if c, ok := e.(map[string]any); ok {
			out = append(out, c)
		}
	}
	return out
}

// defaults fills what the server's defaulting functions set on every write.
// A value a caller declared is never replaced.
func (s *apiServer) defaults(info kindInfo, object *unstructured.Unstructured) {
	_ = canonicalize(object)
	spec := child(object.Object, "spec")
	switch info.kind {
	case "Pod":
		if spec != nil {
			defaultPodSpec(spec)
			setDefault(spec, "enableServiceLinks", true)
		}
	case "Service":
		defaultService(spec)
	case "Deployment":
		if spec != nil {
			setDefault(spec, "replicas", int64(1))
			setDefault(spec, "revisionHistoryLimit", int64(10))
			setDefault(spec, "progressDeadlineSeconds", int64(600))
			strategy := child(spec, "strategy")
			if strategy == nil {
				strategy = map[string]any{}
				spec["strategy"] = strategy
			}
			setDefault(strategy, "type", "RollingUpdate")
			if strategy["type"] == "RollingUpdate" {
				ru := child(strategy, "rollingUpdate")
				if ru == nil {
					ru = map[string]any{}
					strategy["rollingUpdate"] = ru
				}
				setDefault(ru, "maxSurge", "25%")
				setDefault(ru, "maxUnavailable", "25%")
			}
			defaultTemplate(spec)
		}
	case "ReplicaSet":
		if spec != nil {
			setDefault(spec, "replicas", int64(1))
			defaultTemplate(spec)
		}
	case "StatefulSet":
		if spec != nil {
			setDefault(spec, "replicas", int64(1))
			setDefault(spec, "revisionHistoryLimit", int64(10))
			setDefault(spec, "podManagementPolicy", "OrderedReady")
			setDefault(spec, "persistentVolumeClaimRetentionPolicy", map[string]any{"whenDeleted": "Retain", "whenScaled": "Retain"})
			strategy := child(spec, "updateStrategy")
			if strategy == nil {
				strategy = map[string]any{}
				spec["updateStrategy"] = strategy
			}
			setDefault(strategy, "type", "RollingUpdate")
			if strategy["type"] == "RollingUpdate" {
				ru := child(strategy, "rollingUpdate")
				if ru == nil {
					ru = map[string]any{}
					strategy["rollingUpdate"] = ru
				}
				setDefault(ru, "partition", int64(0))
			}
			defaultTemplate(spec)
			for _, claim := range children(spec, "volumeClaimTemplates") {
				defaultClaim(claim)
			}
		}
	case "DaemonSet":
		if spec != nil {
			setDefault(spec, "revisionHistoryLimit", int64(10))
			strategy := child(spec, "updateStrategy")
			if strategy == nil {
				strategy = map[string]any{}
				spec["updateStrategy"] = strategy
			}
			setDefault(strategy, "type", "RollingUpdate")
			if strategy["type"] == "RollingUpdate" {
				ru := child(strategy, "rollingUpdate")
				if ru == nil {
					ru = map[string]any{}
					strategy["rollingUpdate"] = ru
				}
				setDefault(ru, "maxUnavailable", int64(1))
				setDefault(ru, "maxSurge", int64(0))
			}
			defaultTemplate(spec)
		}
	case "Job":
		if spec != nil {
			setDefault(spec, "completions", int64(1))
			setDefault(spec, "parallelism", int64(1))
			setDefault(spec, "backoffLimit", int64(6))
			setDefault(spec, "completionMode", "NonIndexed")
			setDefault(spec, "suspend", false)
			setDefault(spec, "podReplacementPolicy", "TerminatingOrFailed")
			setDefault(spec, "manualSelector", false)
			defaultTemplate(spec)
		}
	case "PersistentVolumeClaim":
		defaultClaim(object.Object)
	case "Secret":
		setDefault(object.Object, "type", "Opaque")
		// stringData is write-only: it is folded into data and never stored.
		if plain, ok := object.Object["stringData"].(map[string]any); ok {
			data, _ := object.Object["data"].(map[string]any)
			if data == nil {
				data = map[string]any{}
				object.Object["data"] = data
			}
			for k, v := range plain {
				if str, ok := v.(string); ok {
					data[k] = base64.StdEncoding.EncodeToString([]byte(str))
				}
			}
			delete(object.Object, "stringData")
		}
	case "NetworkPolicy":
		if spec != nil {
			if _, ok := spec["policyTypes"]; !ok {
				types := []any{"Ingress"}
				if _, egress := spec["egress"]; egress {
					types = append(types, "Egress")
				}
				spec["policyTypes"] = types
			}
		}
	}
}

func defaultClaim(claim map[string]any) {
	spec := child(claim, "spec")
	if spec != nil {
		setDefault(spec, "volumeMode", "Filesystem")
	}
	if claim["kind"] == "PersistentVolumeClaim" {
		status := child(claim, "status")
		if status == nil {
			status = map[string]any{}
			claim["status"] = status
		}
		setDefault(status, "phase", "Pending")
	}
}

func withDefaultTolerations(existing any) []any {
	list, _ := existing.([]any)
	for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
		found := false
		for _, e := range list {
			if t, ok := e.(map[string]any); ok && t["key"] == key && t["effect"] == "NoExecute" {
				found = true
			}
		}
		if !found {
			list = append(list, map[string]any{"key": key, "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": int64(300)})
		}
	}
	return list
}

func defaultTemplate(spec map[string]any) {
	if template := child(spec, "template"); template != nil {
		if podSpec := child(template, "spec"); podSpec != nil {
			defaultPodSpec(podSpec)
		}
	}
}

func defaultPodSpec(spec map[string]any) {
	setDefault(spec, "dnsPolicy", "ClusterFirst")
	if spec["hostNetwork"] == true {
		spec["dnsPolicy"] = "ClusterFirstWithHostNet"
	}
	setDefault(spec, "restartPolicy", "Always")
	setDefault(spec, "schedulerName", "default-scheduler")
	setDefault(spec, "terminationGracePeriodSeconds", int64(30))
	setDefault(spec, "securityContext", map[string]any{})
	for _, list := range []string{"containers", "initContainers"} {
		for _, c := range children(spec, list) {
			defaultContainer(c)
		}
	}
	for _, v := range children(spec, "volumes") {
		defaultVolume(v)
	}
}

func defaultContainer(c map[string]any) {
	setDefault(c, "terminationMessagePath", "/dev/termination-log")
	setDefault(c, "terminationMessagePolicy", "File")
	setDefault(c, "resources", map[string]any{})
	image, _ := c["image"].(string)
	policy := "IfNotPresent"
	name := image
	if i := strings.Index(name, "@"); i >= 0 {
		name = ""
	}
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if image != "" && !strings.Contains(image, "@") && (!strings.Contains(name, ":") || strings.HasSuffix(name, ":latest")) {
		policy = "Always"
	}
	setDefault(c, "imagePullPolicy", policy)
	for _, p := range children(c, "ports") {
		setDefault(p, "protocol", "TCP")
	}
	for _, e := range children(c, "env") {
		if from := child(e, "valueFrom"); from != nil {
			if ref := child(from, "fieldRef"); ref != nil {
				setDefault(ref, "apiVersion", "v1")
			}
		}
	}
	for _, probe := range []string{"livenessProbe", "readinessProbe", "startupProbe"} {
		p := child(c, probe)
		if p == nil {
			continue
		}
		setDefault(p, "timeoutSeconds", int64(1))
		setDefault(p, "periodSeconds", int64(10))
		setDefault(p, "successThreshold", int64(1))
		setDefault(p, "failureThreshold", int64(3))
		if get := child(p, "httpGet"); get != nil {
			setDefault(get, "scheme", "HTTP")
		}
		if grpc := child(p, "grpc"); grpc != nil {
			setDefault(grpc, "service", "")
		}
	}
	if res := child(c, "resources"); res != nil {
		_ = res
	}
}

func defaultVolume(v map[string]any) {
	for _, source := range []string{"configMap", "secret"} {
		if s := child(v, source); s != nil {
			setDefault(s, "defaultMode", int64(420))
		}
	}
	if s := child(v, "downwardAPI"); s != nil {
		setDefault(s, "defaultMode", int64(420))
		for _, item := range children(s, "items") {
			if ref := child(item, "fieldRef"); ref != nil {
				setDefault(ref, "apiVersion", "v1")
			}
		}
	}
	if s := child(v, "projected"); s != nil {
		setDefault(s, "defaultMode", int64(420))
	}
	if s := child(v, "hostPath"); s != nil {
		setDefault(s, "type", "")
	}
}

func defaultService(spec map[string]any) {
	if spec == nil {
		return
	}
	setDefault(spec, "type", "ClusterIP")
	serviceType, _ := spec["type"].(string)
	setDefault(spec, "sessionAffinity", "None")
	if spec["sessionAffinity"] == "ClientIP" {
		config := child(spec, "sessionAffinityConfig")
		if config == nil {
			config = map[string]any{}
			spec["sessionAffinityConfig"] = config
		}
		clientIP := child(config, "clientIP")
		if clientIP == nil {
			clientIP = map[string]any{}
			config["clientIP"] = clientIP
		}
		setDefault(clientIP, "timeoutSeconds", int64(10800))
	}
	for _, p := range children(spec, "ports") {
		setDefault(p, "protocol", "TCP")
		// A typed round trip writes an unset targetPort as 0.
		if tp, ok := p["targetPort"]; (!ok || tp == int64(0) || tp == "") && serviceType != "ExternalName" {
			p["targetPort"] = p["port"]
		}
	}
	if serviceType == "ExternalName" {
		return
	}
	setDefault(spec, "internalTrafficPolicy", "Cluster")
	if serviceType == "NodePort" || serviceType == "LoadBalancer" {
		setDefault(spec, "externalTrafficPolicy", "Cluster")
	}
}

// initialize runs the registry's create-time work: identity-derived fields and
// address allocation.
func (s *apiServer) initialize(info kindInfo, object *unstructured.Unstructured) {
	spec := child(object.Object, "spec")
	switch info.kind {
	case "Service":
		if spec == nil {
			return
		}
		serviceType, _ := spec["type"].(string)
		if serviceType == "ExternalName" {
			return
		}
		setDefault(spec, "ipFamilyPolicy", "SingleStack")
		setDefault(spec, "ipFamilies", []any{"IPv4"})
		if ip, _ := spec["clusterIP"].(string); ip == "" {
			s.clusterIPs++
			ip = fmt.Sprintf("10.96.%d.%d", s.clusterIPs/250, s.clusterIPs%250+1)
			spec["clusterIP"], spec["clusterIPs"] = ip, []any{ip}
		} else if _, ok := spec["clusterIPs"]; !ok {
			spec["clusterIPs"] = []any{ip}
		}
		if serviceType == "NodePort" || serviceType == "LoadBalancer" {
			for _, p := range children(spec, "ports") {
				if _, ok := p["nodePort"]; !ok {
					s.nodePorts++
					p["nodePort"] = int64(30000 + s.nodePorts)
				}
			}
		}
		if _, ok := object.Object["status"]; !ok {
			object.Object["status"] = map[string]any{}
		}
		child(object.Object, "status")["loadBalancer"] = map[string]any{}
	case "Pod":
		// Admission plugins (Priority, DefaultTolerationSeconds) and the registry.
		if spec != nil {
			setDefault(spec, "preemptionPolicy", "PreemptLowerPriority")
			setDefault(spec, "priority", int64(0))
			spec["tolerations"] = withDefaultTolerations(spec["tolerations"])
		}
		status := child(object.Object, "status")
		if status == nil {
			status = map[string]any{}
			object.Object["status"] = status
		}
		status["phase"] = "Pending"
		status["qosClass"] = qosClass(spec)
	case "Node":
		// The TaintNodesByCondition admission plugin.
		spec := child(object.Object, "spec")
		if spec == nil {
			spec = map[string]any{}
			object.Object["spec"] = spec
		}
		spec["taints"] = append(sliceOf(spec["taints"]), map[string]any{"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"})
	case "Job":
		uid := string(object.GetUID())
		labels := object.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		generated := map[string]string{"batch.kubernetes.io/controller-uid": uid, "batch.kubernetes.io/job-name": object.GetName(), "controller-uid": uid, "job-name": object.GetName()}
		for k, v := range generated {
			labels[k] = v
		}
		object.SetLabels(labels)
		if spec != nil {
			if child(spec, "selector") == nil {
				spec["selector"] = map[string]any{"matchLabels": map[string]any{"batch.kubernetes.io/controller-uid": uid}}
			}
			template := child(spec, "template")
			meta := child(template, "metadata")
			if meta == nil {
				meta = map[string]any{}
				template["metadata"] = meta
			}
			tl := map[string]any{}
			if existing := child(meta, "labels"); existing != nil {
				tl = existing
			}
			for k, v := range generated {
				tl[k] = v
			}
			meta["labels"] = tl
		}
	}
}

// qosClass derives the pod QoS class from container requests and limits.
func qosClass(spec map[string]any) string {
	if spec == nil {
		return "BestEffort"
	}
	any1, guaranteed := false, true
	containers := append(children(spec, "initContainers"), children(spec, "containers")...)
	for _, c := range containers {
		res := child(c, "resources")
		requests, limits := child(res, "requests"), child(res, "limits")
		if len(requests) > 0 || len(limits) > 0 {
			any1 = true
		}
		for _, name := range []string{"cpu", "memory"} {
			lim, hasLim := limits[name]
			if !hasLim {
				guaranteed = false
				continue
			}
			if req, hasReq := requests[name]; hasReq && !equalQuantity(req, lim) {
				guaranteed = false
			}
		}
	}
	switch {
	case !any1:
		return "BestEffort"
	case guaranteed && len(containers) > 0:
		return "Guaranteed"
	}
	return "Burstable"
}

func equalQuantity(a, b any) bool {
	x, ok1 := a.(string)
	y, ok2 := b.(string)
	if !ok1 || !ok2 {
		return false
	}
	qa, err1 := resource.ParseQuantity(x)
	qb, err2 := resource.ParseQuantity(y)
	return err1 == nil && err2 == nil && qa.Cmp(qb) == 0
}

func sliceOf(v any) []any {
	list, _ := v.([]any)
	return list
}
