package metrics_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/sdktest"
)

func alertSpec() sdk.DetectorSpec {
	spec := spec()
	spec.ID = "metrics-alerts"
	return spec
}

func firing(name string, labels map[string]string, annotations map[string]string) metrics.Alert {
	merged := map[string]string{"alertname": name}
	for key, value := range labels {
		merged[key] = value
	}
	return metrics.Alert{Labels: merged, Annotations: annotations, State: "firing", ActiveAt: observedAt.Add(-5 * time.Minute), Value: "1e+00"}
}

func withAlerts(alerts ...metrics.Alert) sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "shop", Alerts: &metrics.AlertsResult{ObservedAt: observedAt, Alerts: alerts}}
}

func detectAlerts(t *testing.T, mapping metrics.LabelMapping, ctx sdk.DetectionContext) []sdk.Finding {
	t.Helper()
	findings, err := metrics.NewAlertsDetector(alertSpec(), mapping).Detect(context.Background(), ctx)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func TestAlertsDetectorFiltersByNamespaceAndStateOnly(t *testing.T) {
	pending := firing("HighLatency", map[string]string{"namespace": "shop", "deployment": "web"}, nil)
	pending.State = "pending"
	findings := detectAlerts(t, metrics.LabelMapping{}, withAlerts(
		firing("PodCrashLooping", map[string]string{"namespace": "shop", "pod": "web-1", "severity": "critical"}, nil),
		firing("PodCrashLooping", map[string]string{"namespace": "other", "pod": "web-9", "severity": "critical"}, nil),
		pending,
		firing("NodeDown", map[string]string{"instance": "node-1"}, nil),
		firing("KubeJobFailed", map[string]string{"namespace": "shop", "job_name": "etl"}, nil),
	))
	if len(findings) != 2 {
		t.Fatalf("only firing alerts of the application namespace report: %+v", findings)
	}
	for _, finding := range findings {
		if finding.Metadata["alerts_without_namespace_label"] != 1 {
			t.Errorf("label-less alerts must be counted: %+v", finding.Metadata)
		}
		if finding.Status != sdk.FindingActive || finding.DetectorID != "metrics-alerts" {
			t.Errorf("finding identity: %+v", finding)
		}
	}
	byRule := map[string]sdk.Finding{}
	for _, finding := range findings {
		byRule[finding.RuleID] = finding
	}
	crash := byRule["alert.PodCrashLooping"]
	if want := (sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "shop", Name: "web-1"}); crash.PrimaryResource != want {
		t.Fatalf("primary resource: %+v", crash.PrimaryResource)
	}
	if crash.Severity != sdk.SeverityCritical {
		t.Fatalf("severity: %s", crash.Severity)
	}
	failed := byRule["alert.KubeJobFailed"]
	if failed.Severity != sdk.SeverityWarning || failed.PrimaryResource.Kind != "Namespace" || failed.PrimaryResource.Name != "shop" {
		t.Fatalf("unmapped alerts name the namespace and default to warn: %+v", failed)
	}
}

func TestAlertsDetectorMapsSeverityLabel(t *testing.T) {
	cases := map[string]sdk.FindingSeverity{
		"critical": sdk.SeverityCritical, "warning": sdk.SeverityWarning, "": sdk.SeverityWarning, "page": sdk.SeverityWarning, "info": sdk.SeverityWarning,
	}
	for label, want := range cases {
		labels := map[string]string{"namespace": "shop"}
		if label != "" {
			labels["severity"] = label
		}
		findings := detectAlerts(t, metrics.LabelMapping{}, withAlerts(firing("A", labels, nil)))
		if len(findings) != 1 || findings[0].Severity != want {
			t.Errorf("severity label %q: %+v", label, findings)
		}
	}
}

func TestAlertsDetectorEvidenceCarriesAnnotationsAndLabels(t *testing.T) {
	findings := detectAlerts(t, metrics.LabelMapping{}, withAlerts(firing("HighErrorRate",
		map[string]string{"namespace": "shop", "service": "checkout", "severity": "critical"},
		map[string]string{"summary": "5xx ratio above 5%", "runbook_url": "https://runbooks.example/high-error-rate"})))
	if len(findings) != 1 {
		t.Fatalf("findings: %+v", findings)
	}
	finding := findings[0]
	for _, want := range []string{"HighErrorRate", "5xx ratio above 5%", "runbook_url", `service="checkout"`, "firing", "2026-10-01T11:55:00Z"} {
		if !strings.Contains(finding.Evidence, want) {
			t.Errorf("evidence lacks %q: %s", want, finding.Evidence)
		}
	}
	if finding.RuleID != "alert.HighErrorRate" || finding.PrimaryResource.Kind != "Service" || finding.PrimaryResource.Name != "checkout" {
		t.Fatalf("finding: %+v", finding)
	}
	annotations, _ := finding.Metadata["annotations"].(map[string]string)
	if annotations["summary"] != "5xx ratio above 5%" || finding.Metadata["alert_name"] != "HighErrorRate" {
		t.Fatalf("metadata: %+v", finding.Metadata)
	}
}

func TestAlertsDetectorHonoursConfiguredLabels(t *testing.T) {
	mapping := metrics.LabelMapping{Namespace: "kubernetes_namespace", Deployment: "app"}
	findings := detectAlerts(t, mapping, withAlerts(
		firing("A", map[string]string{"kubernetes_namespace": "shop", "app": "cart"}, nil),
		firing("B", map[string]string{"namespace": "shop"}, nil),
	))
	if len(findings) != 1 || findings[0].PrimaryResource.Name != "cart" || findings[0].Metadata["alerts_without_namespace_label"] != 1 {
		t.Fatalf("findings: %+v", findings)
	}
}

func TestAlertsDetectorFingerprintsAlertInstancesAndSortsOutput(t *testing.T) {
	alerts := []metrics.Alert{
		firing("B", map[string]string{"namespace": "shop", "pod": "p2"}, nil),
		firing("A", map[string]string{"namespace": "shop", "pod": "p2"}, nil),
		firing("A", map[string]string{"namespace": "shop", "pod": "p1"}, nil),
	}
	first := detectAlerts(t, metrics.LabelMapping{}, withAlerts(alerts...))
	second := detectAlerts(t, metrics.LabelMapping{}, withAlerts(alerts[2], alerts[0], alerts[1]))
	seen := map[string]bool{}
	for index := range first {
		if seen[first[index].Fingerprint] || first[index].Fingerprint != second[index].Fingerprint {
			t.Fatalf("fingerprints must be distinct and order independent:\n%+v\n%+v", first, second)
		}
		seen[first[index].Fingerprint] = true
	}
	if first[0].RuleID != "alert.A" || first[2].RuleID != "alert.B" {
		t.Fatalf("output must be sorted: %+v", first)
	}
}

func TestAlertsDetectorUnavailableAlertsAreAnErrorNotClear(t *testing.T) {
	detector := metrics.NewAlertsDetector(alertSpec(), metrics.LabelMapping{})
	cases := map[string]sdk.DetectionContext{
		"read failed": sdktest.Snapshot{NamespaceName: "shop", Alerts: &metrics.AlertsResult{ObservedAt: observedAt, Error: "alerts endpoint HTTP 503"}},
		"no alerts":   sdktest.Snapshot{NamespaceName: "shop"},
		"no source":   plainContext{},
	}
	for name, ctx := range cases {
		findings, err := detector.Detect(context.Background(), ctx)
		if err == nil || len(findings) != 0 {
			t.Errorf("%s: unavailable alerts must be a detector error: %+v %v", name, findings, err)
		}
	}
	if _, err := detector.Detect(context.Background(), cases["read failed"]); !strings.Contains(err.Error(), "503") {
		t.Fatalf("the source's reason must be preserved: %v", err)
	}
}

func TestAlertsDetectorNoAlertsIsHealthy(t *testing.T) {
	if findings := detectAlerts(t, metrics.LabelMapping{}, withAlerts()); len(findings) != 0 {
		t.Fatalf("no alerts is healthy: %+v", findings)
	}
}

func TestAlertsDetectorDeclaresItsInput(t *testing.T) {
	detector := metrics.NewAlertsDetector(alertSpec(), metrics.LabelMapping{})
	consumer, ok := detector.(metrics.AlertConsumer)
	if !ok || !consumer.ConsumesAlerts() {
		t.Fatalf("detector must declare that it consumes alerts: %#v", detector)
	}
	if _, ok := detector.(metrics.Consumer); ok {
		t.Fatal("an alerts detector declares no metric queries")
	}
	if !metrics.ConsumesAlerts([]sdk.Detector{detector}) || metrics.ConsumesAlerts(nil) {
		t.Fatal("ConsumesAlerts must report detector input")
	}
}
