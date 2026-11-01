//go:build integration

package runtime

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"testing"
	"time"
)

func TestUsageLiveKubeletProxy(t *testing.T) {
	if os.Getenv("SDO_USAGE_LIVE_CONTEXT") == "" {
		t.Skip("set SDO_USAGE_LIVE_CONTEXT for read-only kubelet proxy check")
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{CurrentContext: os.Getenv("SDO_USAGE_LIVE_CONTEXT")}).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pods, err := client.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) == 0 {
		t.Fatal("no kube-system pods")
	}
	observer := usageSnapshotProvider{client: client}
	data, err := observer.read(ctx, pods.Items[0].Spec.NodeName, "stats/summary")
	if err != nil {
		t.Fatal(err)
	}
	summary, err := parseUsageSummary(data, "kube-system", pods.Items, time.Now())
	if err != nil || len(summary.Containers) == 0 {
		t.Fatal(summary, err)
	}
	data, err = observer.read(ctx, pods.Items[0].Spec.NodeName, "metrics/cadvisor")
	if err != nil {
		t.Fatal(err)
	}
	counters, err := parseUsageCounters(data, "kube-system")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Summary: %d containers; CFS: %d identities", len(summary.Containers), len(counters))
}
