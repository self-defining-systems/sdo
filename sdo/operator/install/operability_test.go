package install

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/operability"
)

func operabilityReport(t *testing.T, commit string) operability.Report {
	t.Helper()
	subject := operability.Subject{AcceptedCommit: commit, BaseTree: "b", EffectiveTree: "e", PatchesDigest: "p", RenderDigest: "r", EnvDigest: "v", DiagnosticsTree: "d"}
	var checks []operability.Check
	for _, id := range operability.AllItems() {
		checks = append(checks, operability.Check{ID: string(id) + ".ok", Phase: operability.PhaseLive, Status: operability.StatusPass})
	}
	report, err := operability.New(operability.StageComplete, subject).With(checks...)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

// The summary is current state: created once, then replaced by the next report,
// in the control namespace with the managed-by label.
func TestPublishOperabilityCreatesThenReplacesTheSummary(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	for _, commit := range []string{"c1", "c2"} {
		if err := PublishOperability(ctx, client, "sdo-control", operabilityReport(t, commit), time.Unix(0, 0)); err != nil {
			t.Fatal(err)
		}
	}
	configMap, err := client.CoreV1().ConfigMaps("sdo-control").Get(ctx, OperabilityConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if configMap.Labels["app.kubernetes.io/managed-by"] != "sdo" {
		t.Fatalf("labels = %v", configMap.Labels)
	}
	var summary operability.Summary
	if err := json.Unmarshal([]byte(configMap.Data["summary.json"]), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.AcceptedCommit != "c2" || summary.Stage != operability.StageComplete || len(summary.Items) != 6 {
		t.Fatalf("summary = %+v, want the second report", summary)
	}
	if _, err := operability.Decode([]byte(configMap.Data["report.json"])); err != nil {
		t.Fatalf("report.json does not decode: %v", err)
	}
}

func TestPublishOperabilityRefusesAnInvalidReport(t *testing.T) {
	if err := PublishOperability(context.Background(), fake.NewSimpleClientset(), "ns", operability.Report{}, time.Unix(0, 0)); err == nil {
		t.Fatal("an empty report was published")
	}
	if err := PublishOperability(context.Background(), nil, "ns", operabilityReport(t, "c"), time.Unix(0, 0)); err == nil {
		t.Fatal("published without a client")
	}
}

// Open upstream obligations sit beside the report under their own key. A new
// report replaces the summary but never drops them, and nothing is created
// before a report exists (the ConfigMap would have no summary to read).
func TestOperabilityObligationsSurviveANewReport(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	if err := PublishOperabilityObligations(ctx, client, "sdo-control", `[{"kind":"designer_refresh"}]`); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("sdo-control").Get(ctx, OperabilityConfigMap, metav1.GetOptions{}); err == nil {
		t.Fatal("obligations created a report ConfigMap with no report")
	}
	if err := PublishOperability(ctx, client, "sdo-control", operabilityReport(t, "c1"), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := PublishOperabilityObligations(ctx, client, "sdo-control", `[{"kind":"designer_refresh"}]`); err != nil {
		t.Fatal(err)
	}
	if err := PublishOperability(ctx, client, "sdo-control", operabilityReport(t, "c2"), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	configMap, err := client.CoreV1().ConfigMaps("sdo-control").Get(ctx, OperabilityConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := configMap.Data[OperabilityObligationsKey]; got != `[{"kind":"designer_refresh"}]` {
		t.Fatalf("obligations = %q after a new report", got)
	}
	if configMap.Data["summary.json"] == "" || configMap.Data["report.json"] == "" {
		t.Fatalf("report keys lost: %v", configMap.Data)
	}
}
