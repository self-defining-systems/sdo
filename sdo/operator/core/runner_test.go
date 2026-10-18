package core

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type testDetector struct {
	id string
}

func (d testDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Interval: time.Second}
}

func (d testDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{
		{
			RuleID:   d.id,
			Status:   sdk.FindingActive,
			Severity: sdk.SeverityWarning,
			Summary:  "test finding",
			Evidence: "test evidence",
			PrimaryResource: sdk.ObjectRef{
				Kind: "Service",
				Name: "api",
			},
		},
	}, nil
}

type findingsDetector struct {
	id        string
	findings  []sdk.Finding
	playbooks []string
}

func (d findingsDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Interval: time.Second, Playbooks: d.playbooks}
}

func (d findingsDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.findings, nil
}

func TestValidateDetectorsRejectsDuplicateIDs(t *testing.T) {
	err := ValidateDetectors([]sdk.Detector{
		testDetector{id: "same"},
		testDetector{id: "same"},
	})
	if err == nil {
		t.Fatal("expected duplicate detector error")
	}
}

func TestRunnerEmitsFindingsAsJSON(t *testing.T) {
	var output bytes.Buffer
	runner := Runner{
		Detectors: []sdk.Detector{testDetector{id: "missing-endpoints"}},
		Provider:  StaticSnapshotProvider{SnapshotValue: sdktest.Snapshot{NamespaceName: "demo"}},
		Sink:      JSONSink{Writer: &output},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run once: %v", err)
	}
	if !bytes.Contains(output.Bytes(), []byte(`"rule_id":"missing-endpoints"`)) {
		t.Fatalf("expected JSON finding, got %s", output.String())
	}
	if !bytes.Contains(output.Bytes(), []byte(`"detector_id":"missing-endpoints"`)) {
		t.Fatalf("expected JSON finding to include detector id, got %s", output.String())
	}
}

func TestRunWithOptionsUsesInjectedLogicalPlaybookResolver(t *testing.T) {
	var stdout bytes.Buffer
	const legacy = ".sdo/playbooks/service-endpoints/README.md"
	err := RunWithOptions(context.Background(), []sdk.Detector{
		findingsDetector{id: "missing-endpoints", playbooks: []string{legacy}, findings: []sdk.Finding{{
			RuleID: "missing-endpoints", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
			Summary: "test finding", Evidence: "test evidence", Playbooks: []string{legacy},
			PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "api"},
		}}},
	}, RuntimeOptions{
		Args: []string{}, Stdout: &stdout, Provider: StaticSnapshotProvider{SnapshotValue: sdktest.Snapshot{NamespaceName: "demo"}},
		PlaybookResolver: func(references []string) ([]string, error) {
			if len(references) != 1 || references[0] != legacy {
				t.Fatalf("resolver received wrong references: %v", references)
			}
			return []string{"service-endpoints"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"playbooks":["service-endpoints"]`)) || bytes.Contains(stdout.Bytes(), []byte(".sdo/")) {
		t.Fatalf("core did not emit logical IDs: %s", stdout.String())
	}
}

func TestRunWithOptionsWithoutResolverRejectsPhysicalPlaybook(t *testing.T) {
	const legacy = ".sdo/playbooks/service-endpoints/README.md"
	err := RunWithOptions(context.Background(), []sdk.Detector{
		findingsDetector{id: "missing-endpoints", playbooks: []string{legacy}, findings: []sdk.Finding{{
			RuleID: "missing-endpoints", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
			Summary: "test finding", Evidence: "test evidence", Playbooks: []string{legacy},
			PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "api"},
		}}},
	}, RuntimeOptions{Args: []string{}, Provider: StaticSnapshotProvider{SnapshotValue: sdktest.Snapshot{NamespaceName: "demo"}}})
	if err == nil {
		t.Fatal("core emitted a physical playbook reference without a storage-edge resolver")
	}
}

func TestRunWithOptionsRejectsInvalidFindingPlaybook(t *testing.T) {
	err := RunWithOptions(context.Background(), []sdk.Detector{
		findingsDetector{
			id:        "missing-endpoints",
			playbooks: []string{".sdo/playbooks/missing/README.md"},
			findings: []sdk.Finding{
				{
					RuleID:    "missing-endpoints",
					Status:    sdk.FindingActive,
					Severity:  sdk.SeverityWarning,
					Summary:   "test finding",
					Evidence:  "test evidence",
					Playbooks: []string{".sdo/playbooks/missing/README.md"},
					PrimaryResource: sdk.ObjectRef{
						Kind: "Service",
						Name: "api",
					},
				},
			},
		},
	}, RuntimeOptions{
		Args:             []string{},
		Provider:         StaticSnapshotProvider{SnapshotValue: sdktest.Snapshot{NamespaceName: "demo"}},
		PlaybookResolver: func([]string) ([]string, error) { return nil, errors.New("unknown playbook ID") },
	})
	if err == nil {
		t.Fatal("expected missing playbook error")
	}
}

func TestRunWithOptionsCanValidateDetectorsOnly(t *testing.T) {
	var stdout bytes.Buffer
	err := RunWithOptions(context.Background(), []sdk.Detector{testDetector{id: "ok"}}, RuntimeOptions{
		Args:   []string{"--validate-detectors"},
		Stdout: &stdout,
	})
	if err != nil {
		t.Fatalf("validate detectors: %v", err)
	}
	if got := stdout.String(); got != "validated 1 detector(s)\n" {
		t.Fatalf("unexpected output %q", got)
	}
}
