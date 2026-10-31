package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Transport executes the real Kubernetes clients against the world's objects,
// kubelet samples and raw log tails. No listener, socket or process is created.
type Transport struct{ World *World }

var resources = map[string]string{"configmaps": "ConfigMap", "services": "Service", "pods": "Pod", "deployments": "Deployment", "replicasets": "ReplicaSet", "statefulsets": "StatefulSet", "daemonsets": "DaemonSet", "jobs": "Job", "cronjobs": "CronJob", "persistentvolumeclaims": "PersistentVolumeClaim", "serviceaccounts": "ServiceAccount", "nodes": "Node", "secrets": "Secret", "ingresses": "Ingress", "endpoints": "Endpoints", "endpointslices": "EndpointSlice", "networkpolicies": "NetworkPolicy", "horizontalpodautoscalers": "HorizontalPodAutoscaler", "poddisruptionbudgets": "PodDisruptionBudget", "events": "Event"}

// Client returns a real client-go client using only the in-memory transport.
func (w *World) Client() (kubernetes.Interface, error) {
	return kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://simulation", ContentConfig: rest.ContentConfig{ContentType: "application/json"}}, &http.Client{Transport: Transport{w}})
}
func (tr Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	w := tr.World
	path := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	edge := "api"
	for _, p := range path {
		if p == "log" {
			edge = "logs"
		}
		if p == "proxy" {
			edge = "kubelet"
		}
	}
	if err := w.Read(req.Context(), edge); err != nil {
		return nil, err
	}
	reply := func(code int, data []byte) *http.Response {
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), Request: req}
	}
	if edge == "logs" {
		ns, pod := "", ""
		for i, p := range path {
			if p == "namespaces" && i+1 < len(path) {
				ns = path[i+1]
			}
			if p == "pods" && i+1 < len(path) {
				pod = path[i+1]
			}
		}
		key := LogKey{ns, pod, req.URL.Query().Get("container"), req.URL.Query().Get("previous") == "true"}
		data, ok := w.Logs[key]
		if !ok {
			return reply(404, []byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)), nil
		}
		return reply(200, []byte(data)), nil
	}
	if edge == "kubelet" {
		node := ""
		for i, p := range path {
			if p == "nodes" && i+1 < len(path) {
				node = path[i+1]
			}
		}
		data := w.Kubelet[node]
		if strings.HasSuffix(req.URL.Path, "/metrics/cadvisor") {
			return reply(200, []byte(data.CAdvisor)), nil
		}
		return reply(200, data.Summary), nil
	}
	if req.Method != "GET" {
		return nil, fmt.Errorf("simulation transport: unsupported %s %s", req.Method, req.URL.Path)
	}
	apiVersion := path[1]
	if path[0] == "apis" {
		apiVersion = path[1] + "/" + path[2]
	}
	plural := path[len(path)-1]
	kind, ok := resources[plural]
	if !ok {
		return nil, fmt.Errorf("simulation transport: unknown resource %s", plural)
	}
	ns := ""
	for i, p := range path {
		if p == "namespaces" && i+1 < len(path) {
			ns = path[i+1]
		}
	}
	items := []any{}
	for _, o := range w.Kube.Objects() {
		if o.GetKind() == kind && (ns == "" || ns == o.GetNamespace()) {
			if kind == "Secret" {
				var secret corev1.Secret
				_ = json.Unmarshal(mustJSON(o.Object), &secret)
				for key := range secret.Data {
					secret.Data[key] = nil
				}
				for key := range secret.StringData {
					secret.StringData[key] = ""
				}
				items = append(items, secret)
			} else {
				items = append(items, o.Object)
			}
		}
	}
	return reply(200, mustJSON(map[string]any{"apiVersion": apiVersion, "kind": kind + "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})), nil
}
func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// LogKey identifies current or previous-container raw timestamped log bytes.
type LogKey struct {
	Namespace, Pod, Container string
	Previous                  bool
}

// KubeletSample holds the actual summary/cAdvisor wire formats consumed by usage.
type KubeletSample struct {
	Summary  json.RawMessage
	CAdvisor string
}

// ReadLogs is a narrow seam for readers that do not use client-go.
func (w *World) ReadLogs(ctx context.Context, key LogKey) (io.ReadCloser, error) {
	if err := w.Read(ctx, "logs"); err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader(w.Logs[key])), nil
}
