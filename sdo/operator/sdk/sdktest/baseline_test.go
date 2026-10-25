package sdktest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/routing"
)

const healthyServiceSnapshot = `{
  "namespace": "shop",
  "services": [
    {"metadata": {"name": "cart", "namespace": "shop"}, "spec": {"selector": {"app": "cart"}}}
  ],
  "pods": [
    {"metadata": {"name": "cart-0", "namespace": "shop", "labels": {"app": "cart"}}}
  ]
}`

func writeBaseline(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestLoadSnapshotsReadsSortedJSONFiles(t *testing.T) {
	dir := writeBaseline(t, map[string]string{
		"b.json": strings.Replace(healthyServiceSnapshot, "shop", "later", 1),
		"a.json": healthyServiceSnapshot,
		"note":   "ignored",
	})
	snapshots, err := LoadSnapshots(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 || snapshots[0].Namespace() != "shop" || snapshots[1].Namespace() != "later" {
		t.Fatalf("snapshots = %#v", snapshots)
	}
	if got := snapshots[0].Services(); len(got) != 1 || got[0].Name != "cart" {
		t.Fatalf("services = %#v", got)
	}
	if got := snapshots[0].Pods(); len(got) != 1 || got[0].Labels["app"] != "cart" {
		t.Fatalf("pods = %#v", got)
	}
}

func TestLoadSnapshotsRejectsEmptyAndMalformedBaselines(t *testing.T) {
	if _, err := LoadSnapshots(t.TempDir()); err == nil {
		t.Fatal("empty baseline directory must be an error")
	}
	if _, err := LoadSnapshots(writeBaseline(t, map[string]string{"bad.json": "{"})); err == nil ||
		!strings.Contains(err.Error(), "bad.json") {
		t.Fatalf("malformed baseline error = %v", err)
	}
	if _, err := LoadSnapshots(writeBaseline(t, map[string]string{"x.json": `{"namespace":"a","podz":[]}`})); err == nil {
		t.Fatal("unknown snapshot field must be an error")
	}
}

func TestHealthyBaselineViolationsReportsActiveFindingsOnQuietSnapshot(t *testing.T) {
	snapshots, err := LoadSnapshots(writeBaseline(t, map[string]string{"healthy.json": healthyServiceSnapshot}))
	if err != nil {
		t.Fatal(err)
	}
	noisy := serviceRuleDetector{
		class: sdk.DetectorClassIncident,
		rules: map[string]serviceRule{"always": func(sdk.DetectionContext, corev1.Service) (string, bool) { return "fires", true }},
	}
	violations := HealthyBaselineViolations(noisy, snapshots)
	if len(violations) != 1 {
		t.Fatalf("violations = %v", violations)
	}
	for _, want := range []string{`"fixture"`, "healthy.json", "always", "Service shop/cart", "healthy baseline"} {
		if !strings.Contains(violations[0], want) {
			t.Fatalf("violation %q lacks %q", violations[0], want)
		}
	}
	quiet := serviceRuleDetector{
		class: sdk.DetectorClassIncident,
		rules: map[string]serviceRule{"never": func(sdk.DetectionContext, corev1.Service) (string, bool) { return "", false }},
	}
	if got := HealthyBaselineViolations(quiet, snapshots); len(got) != 0 {
		t.Fatalf("quiet detector violations = %v", got)
	}
}

func TestHealthyBaselineViolationsIgnoresResolvedFindings(t *testing.T) {
	snapshots, err := LoadSnapshots(writeBaseline(t, map[string]string{"healthy.json": healthyServiceSnapshot}))
	if err != nil {
		t.Fatal(err)
	}
	if got := HealthyBaselineViolations(resolvedDetector{}, snapshots); len(got) != 0 {
		t.Fatalf("resolved findings must not violate: %v", got)
	}
}

type resolvedDetector struct{}

func (resolvedDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "resolved", Class: sdk.DetectorClassIncident}
}

func (resolvedDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{RuleID: "r", Status: sdk.FindingResolved}}, nil
}

type repeatingDetector struct{}

func (repeatingDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "repeating", Class: sdk.DetectorClassIncident}
}

func (repeatingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	finding := sdk.Finding{
		RuleID: "r", Status: sdk.FindingActive, Summary: "same",
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "shop", Name: "cart"},
	}
	return []sdk.Finding{finding, finding, finding}, nil
}

func TestHealthyBaselineViolationsReportsTheSameFindingOncePerSnapshot(t *testing.T) {
	snapshots, err := LoadSnapshots(writeBaseline(t, map[string]string{
		"a.json": healthyServiceSnapshot,
		"b.json": healthyServiceSnapshot,
	}))
	if err != nil {
		t.Fatal(err)
	}
	// A detector that emits one finding per matching Deployment would otherwise repeat the
	// message dozens of times and bury the remedy the reflector needs to read.
	if got := HealthyBaselineViolations(repeatingDetector{}, snapshots); len(got) != 2 {
		t.Fatalf("violations = %d, want one per snapshot: %v", len(got), got)
	}
}

type baselineFailureDetector struct{ scoped bool }

func (d baselineFailureDetector) Spec() sdk.DetectorSpec {
	spec := sdk.DetectorSpec{ID: "learned-baseline", Class: sdk.DetectorClassIncident}
	if d.scoped {
		spec.Watches = []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "dependency"}}
	}
	return spec
}
func (d baselineFailureDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.scoped {
		return nil, nil
	}
	return nil, fmt.Errorf("accepted manifest: trusted deployment image evidence missing")
}

func TestHealthyBaselineRejectsDetectorErrorsWithSnapshotContext(t *testing.T) {
	snapshots := []Snapshot{{SourceName: "healthy-a.json", NamespaceName: "app"}, {SourceName: "healthy-b.json", NamespaceName: "app"}}
	for _, tc := range []struct {
		name       string
		detector   sdk.Detector
		diagnostic string
	}{
		{"source gap", baselineFailureDetector{}, "trusted deployment image evidence missing"},
		{"undeclared observation", baselineFailureDetector{scoped: true}, "observation dependency undeclared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := HealthyBaselineViolations(tc.detector, snapshots)
			if len(got) != 2 {
				t.Fatalf("healthy baseline silently accepted an erroring detector: %v", got)
			}
			for i, violation := range got {
				for _, detail := range []string{"learned-baseline", snapshots[i].SourceName, "app", tc.diagnostic} {
					if !strings.Contains(violation, detail) {
						t.Errorf("diagnostic %q lacks %q", violation, detail)
					}
				}
			}
		})
	}
}

func TestHealthyBaselineRejectsInconclusivePromotedRoutingDetector(t *testing.T) {
	snapshots, err := LoadSnapshots(writeBaseline(t, map[string]string{"healthy.json": healthyServiceSnapshot}))
	if err != nil {
		t.Fatal(err)
	}
	detector := program.DetectorFromProgram(routing.Program{}, func(snapshot sdk.DetectionContext) []program.Bindings {
		var bindings []program.Bindings
		for _, service := range snapshot.Services() {
			bindings = append(bindings, program.Bindings{Objects: program.ObjectBindings{"service": sdk.ObjectRefFrom("Service", "v1", &service)}})
		}
		return bindings
	}, sdk.DetectorSpec{ID: "learned-routing", Class: sdk.DetectorClassIncident})
	violations := HealthyBaselineViolations(detector, snapshots)
	if len(violations) != 1 {
		t.Fatalf("unsupported healthy Service silently passed: %v", violations)
	}
	for _, detail := range []string{"learned-routing", "healthy.json", "service-targetport-mismatch", "cart", "Unknown"} {
		if !strings.Contains(violations[0], detail) {
			t.Errorf("diagnostic %q lacks %q", violations[0], detail)
		}
	}
}
