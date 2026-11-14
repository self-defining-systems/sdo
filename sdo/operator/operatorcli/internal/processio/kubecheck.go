package processio

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// KubeCluster reaches one cluster from a kubeconfig: HTTP through the API
// server's Service proxy, and the exposure facts of its Services.
type KubeCluster struct {
	client  *http.Client
	host    string
	kube    kubernetes.Interface
	dynamic dynamic.Interface
}

// OpenKubeCluster loads kubeconfig (a path) and an optional context name.
func OpenKubeCluster(kubeconfig, kubeContext string) (*KubeCluster, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: kubeContext}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	cfg.Timeout = 30 * time.Second
	client, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &KubeCluster{client: client, host: strings.TrimRight(cfg.Host, "/"), kube: kube, dynamic: dyn}, nil
}

// Do sends the request with the cluster credentials.
func (k *KubeCluster) Do(req *http.Request) (*http.Response, error) { return k.client.Do(req) }

// ServiceURL addresses a Service port through the API server proxy.
func (k *KubeCluster) ServiceURL(namespace, service string, port int, path string) string {
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return fmt.Sprintf("%s/api/v1/namespaces/%s/services/%s:%d/proxy%s", k.host, namespace, service, port, path)
}

// ExposedService is a Service reachable from outside the cluster.
type ExposedService struct{ Service, Via string }

// ExposedServices lists Services of the namespace exposed as NodePort or
// LoadBalancer, Ingress backends and Gateway API route backends. A Service whose
// ports are all named for metrics is not an entry.
func (k *KubeCluster) ExposedServices(ctx context.Context, namespace string) ([]ExposedService, error) {
	var out []ExposedService
	seen := map[string]bool{}
	add := func(service, via string) {
		if service != "" && !seen[service+"|"+via] {
			seen[service+"|"+via] = true
			out = append(out, ExposedService{service, via})
		}
	}
	services, err := k.kube.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, s := range services.Items {
		if s.Spec.Type != corev1.ServiceTypeNodePort && s.Spec.Type != corev1.ServiceTypeLoadBalancer {
			continue
		}
		for _, p := range s.Spec.Ports {
			if !strings.Contains(p.Name, "metrics") {
				add(s.Name, string(s.Spec.Type))
				break
			}
		}
	}
	ingresses, err := k.kube.NetworkingV1().Ingresses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, ing := range ingresses.Items {
		if ing.Spec.DefaultBackend != nil && ing.Spec.DefaultBackend.Service != nil {
			add(ing.Spec.DefaultBackend.Service.Name, "Ingress")
		}
		for _, rule := range ing.Spec.Rules {
			if rule.HTTP == nil {
				continue
			}
			for _, path := range rule.HTTP.Paths {
				if path.Backend.Service != nil {
					add(path.Backend.Service.Name, "Ingress")
				}
			}
		}
	}
	// Gateway API routes; the CRDs may be absent, which means no routes.
	for _, kind := range []string{"httproutes", "grpcroutes"} {
		gvr := schema.GroupVersionResource{Group: "gateway.networking.k8s.io", Version: "v1", Resource: kind}
		list, err := k.dynamic.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}
		for _, route := range list.Items {
			rules, _, _ := nestedSlice(route.Object, "spec", "rules")
			for _, rule := range rules {
				refs, _, _ := nestedSlice(asMap(rule), "backendRefs")
				for _, ref := range refs {
					if name, _ := asMap(ref)["name"].(string); name != "" {
						add(name, "Gateway")
					}
				}
			}
		}
	}
	return out, nil
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

func nestedSlice(obj map[string]any, path ...string) ([]any, bool, error) {
	var cur any = obj
	for _, p := range path {
		m := asMap(cur)
		if m == nil {
			return nil, false, nil
		}
		cur = m[p]
	}
	s, ok := cur.([]any)
	return s, ok, nil
}

// Wait pauses for d or until ctx ends.
func Wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
