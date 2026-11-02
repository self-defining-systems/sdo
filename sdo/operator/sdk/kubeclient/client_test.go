package kubeclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/sdk/kubeclient"
)

func TestClientGoSelectorsPaginationAndCRDReads(t *testing.T) {
	var paths []string
	client, err := kubeclient.NewReadClient(func(ctx context.Context, method, path string, q url.Values) ([]byte, error) {
		paths = append(paths, path)
		if method != "GET" {
			t.Fatalf("read client made %s request", method)
		}
		switch path {
		case "/apis/apps/v1/namespaces/app/deployments":
			if q.Get("labelSelector") != "role=api" || q.Get("fieldSelector") != "metadata.name=api" || q.Get("limit") != "17" || q.Get("continue") != "next" {
				t.Fatalf("options lost: %v", q)
			}
			return []byte(`{"apiVersion":"apps/v1","kind":"DeploymentList","metadata":{"continue":"last"},"items":[{"metadata":{"name":"api"},"spec":{"replicas":3}}]}`), nil
		case "/apis/example.test/v1/namespaces/app/widgets/w":
			return []byte(`{"apiVersion":"example.test/v1","kind":"Widget","metadata":{"name":"w"},"spec":{"arbitrary":true}}`), nil
		default:
			return nil, fmt.Errorf("uncaptured request %s", path)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	list, err := client.AppsV1().Deployments("app").List(context.Background(), metav1.ListOptions{LabelSelector: "role=api", FieldSelector: "metadata.name=api", Limit: 17, Continue: "next"})
	if err != nil || list == nil {
		t.Fatalf("list: %v %v", list, err)
	}
	if *list.Items[0].Spec.Replicas != 3 || list.Continue != "last" {
		t.Fatalf("typed result lost: %+v", list)
	}
	item, err := client.Dynamic().Resource(schema.GroupVersionResource{Group: "example.test", Version: "v1", Resource: "widgets"}).Namespace("app").Get(context.Background(), "w", metav1.GetOptions{})
	if err != nil || item.GetName() != "w" {
		t.Fatalf("dynamic read: %v %v", item, err)
	}
	if len(paths) != 2 {
		t.Fatal(paths)
	}
}

func TestReadClientHidesMutationMethods(t *testing.T) {
	client, err := kubeclient.NewReadClient(func(context.Context, string, string, url.Values) ([]byte, error) { return json.RawMessage(`{}`), nil })
	if err != nil {
		t.Fatal(err)
	}
	// The public resource interfaces have no methods accepting mutation options.
	_ = client.CoreV1().Pods("app").Get
	_ = client.AppsV1().Deployments("app").GetScale
}

func TestDiscoveryMapperCachesCustomKinds(t *testing.T) {
	var calls atomic.Int32
	client, err := kubeclient.NewReadClient(func(_ context.Context, _ string, path string, _ url.Values) ([]byte, error) {
		calls.Add(1)
		switch path {
		case "/api":
			return []byte(`{"kind":"APIVersions","apiVersion":"v1","versions":["v1"]}`), nil
		case "/api/v1":
			return []byte(`{"kind":"APIResourceList","groupVersion":"v1","resources":[]}`), nil
		case "/apis":
			return []byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"example.test","versions":[{"groupVersion":"example.test/v1","version":"v1"}],"preferredVersion":{"groupVersion":"example.test/v1","version":"v1"}}]}`), nil
		case "/apis/example.test/v1":
			return []byte(`{"kind":"APIResourceList","groupVersion":"example.test/v1","resources":[{"name":"widgets","singularName":"widget","namespaced":true,"kind":"Widget","verbs":["get","list","watch"]}]}`), nil
		default:
			return nil, fmt.Errorf("unexpected discovery %s", path)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.RESTMapper().RESTMapping(schema.GroupKind{Group: "example.test", Kind: "Widget"}, "v1")
	if err != nil || first.Resource.Resource != "widgets" {
		t.Fatalf("mapping: %v %v", first, err)
	}
	captured := calls.Load()
	if _, err := client.RESTMapper().RESTMapping(schema.GroupKind{Group: "example.test", Kind: "Widget"}, "v1"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != captured {
		t.Fatalf("discovery was not cached: %d -> %d", captured, calls.Load())
	}
}

func TestResourceMetricsKeepUpstreamQuantities(t *testing.T) {
	client, err := kubeclient.NewReadClient(func(_ context.Context, _ string, path string, _ url.Values) ([]byte, error) {
		if path != "/apis/metrics.k8s.io/v1beta1/namespaces/app/pods/api" {
			return nil, fmt.Errorf("wrong metrics path %s", path)
		}
		return []byte(`{"apiVersion":"metrics.k8s.io/v1beta1","kind":"PodMetrics","metadata":{"name":"api","namespace":"app"},"containers":[{"name":"api","usage":{"cpu":"250m","memory":"32Mi"}}]}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := client.Metrics().MetricsV1beta1().PodMetricses("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cpu := metrics.Containers[0].Usage.Cpu()
	if cpu.MilliValue() != 250 {
		t.Fatalf("upstream quantity changed: %v", cpu)
	}
}
