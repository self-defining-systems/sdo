package install

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"strings"
	"testing"
)

func TestObservabilityPlanAndApply(t *testing.T) {
	cfg := testConfig()
	plan, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Prometheus == nil || plan.PrometheusConfig == nil || plan.PrometheusService == nil {
		t.Fatal("default plan lacks Prometheus")
	}
	if got := valueAfter(plan.Job.Spec.Template.Spec.Containers[0].Args, "--metrics-url"); got != cfg.EffectiveMetricsURL() {
		t.Fatalf("controller URL = %q", got)
	}
	if *plan.Prometheus.Spec.Replicas != 1 {
		t.Fatal("expected single replica")
	}
	config := plan.PrometheusConfig.Data["prometheus.yml"]
	for _, want := range []string{"namespaces: {names: [demo]}", "prometheus_io_scrape", "prometheus_io_port", "prometheus_io_path", "/metrics/cadvisor", "__meta_kubernetes_pod_node_name"} {
		if !strings.Contains(config, want) {
			t.Fatalf("missing %q", want)
		}
	}
	client := fake.NewSimpleClientset()
	cfg.Credentials = map[string][]byte{"token": []byte("synthetic")}
	if err := (Installer{Client: client}).ensureBase(context.Background(), cfg, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AppsV1().Deployments(cfg.controlNamespace()).Get(context.Background(), PrometheusName, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	// Reconciliation is idempotent.
	if err := (Installer{Client: client}).ensureBase(context.Background(), cfg, plan); err != nil {
		t.Fatal(err)
	}
	cfg.DisableObservability = true
	plan, err = Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Prometheus != nil || cfg.EffectiveMetricsURL() != "" {
		t.Fatal("opt-out ignored")
	}
	cfg.MetricsURL = "http://external:9090"
	if cfg.EffectiveMetricsURL() != cfg.MetricsURL {
		t.Fatal("explicit URL must win")
	}
}
