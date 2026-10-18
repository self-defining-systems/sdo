package core

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

func TestValidateDetectorsAcceptsTheSyntheticTrafficWatch(t *testing.T) {
	detector := validationDetector{spec: sdk.DetectorSpec{
		ID: "traffic", Interval: time.Second, Watches: []sdk.WatchKind{traffic.Watch},
	}}
	if err := ValidateDetectors([]sdk.Detector{detector}); err != nil {
		t.Fatalf("synthetic traffic watch must be supported: %v", err)
	}
}

type validationDetector struct {
	spec sdk.DetectorSpec
}

func (d validationDetector) Spec() sdk.DetectorSpec {
	return d.spec
}

func (d validationDetector) Detect(_ context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

func TestValidateDetectorsRejectsInvalidIntervals(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			detector := validationDetector{spec: sdk.DetectorSpec{ID: "invalid-interval", Interval: interval}}
			if err := ValidateDetectors([]sdk.Detector{detector}); err == nil {
				t.Fatal("expected invalid interval error")
			}
		})
	}
}

func TestValidateDetectorsRejectsNegativePersistenceMinDuration(t *testing.T) {
	detector := validationDetector{spec: sdk.DetectorSpec{
		ID: "health", Interval: time.Second, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2, MinDuration: -time.Second},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical},
		OriginatingCommit: "commit",
	}}
	if err := ValidateDetectors([]sdk.Detector{detector}); err == nil {
		t.Fatal("expected negative persistence min duration to be rejected")
	}
}

func TestValidateDetectorsAcceptsPositivePersistenceMinDuration(t *testing.T) {
	detector := validationDetector{spec: sdk.DetectorSpec{
		ID: "health", Interval: time.Second, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2, MinDuration: 9 * time.Second},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical},
		OriginatingCommit: "commit",
	}}
	if err := ValidateDetectors([]sdk.Detector{detector}); err != nil {
		t.Fatalf("positive persistence min duration must be accepted: %v", err)
	}
}

func TestValidateDetectorsRejectsUnsupportedAndDuplicateWatches(t *testing.T) {
	tests := []struct {
		name    string
		watches []sdk.WatchKind
	}{
		{
			name:    "unsupported",
			watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Secret"}},
		},
		{
			name: "duplicate",
			watches: []sdk.WatchKind{
				{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo"},
				{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			detector := validationDetector{spec: sdk.DetectorSpec{ID: test.name, Interval: time.Second, Watches: test.watches}}
			if err := ValidateDetectors([]sdk.Detector{detector}); err == nil {
				t.Fatalf("expected %s watch error", test.name)
			}
		})
	}
}

func TestValidateDetectorsAcceptsNetworkPolicyWatch(t *testing.T) {
	detector := validationDetector{spec: sdk.DetectorSpec{
		ID: "network-health", Interval: time.Second,
		Watches: []sdk.WatchKind{{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"}},
	}}
	if err := ValidateDetectors([]sdk.Detector{detector}); err != nil {
		t.Fatalf("NetworkPolicy watch was rejected: %v", err)
	}
}

func TestValidateFindingRejectsMalformedOutput(t *testing.T) {
	spec := sdk.DetectorSpec{
		ID:        "missing-configmap",
		Interval:  time.Second,
		Playbooks: []string{".sdo/playbooks/missing-configmap/README.md"},
	}
	valid := validFinding()
	tests := []struct {
		name   string
		mutate func(*sdk.Finding)
	}{
		{name: "empty rule", mutate: func(f *sdk.Finding) { f.RuleID = "" }},
		{name: "wrong detector", mutate: func(f *sdk.Finding) { f.DetectorID = "other" }},
		{name: "invalid status", mutate: func(f *sdk.Finding) { f.Status = "unknown" }},
		{name: "invalid severity", mutate: func(f *sdk.Finding) { f.Severity = "urgent" }},
		{name: "empty summary", mutate: func(f *sdk.Finding) { f.Summary = "" }},
		{name: "empty evidence", mutate: func(f *sdk.Finding) { f.Evidence = "" }},
		{name: "empty primary kind", mutate: func(f *sdk.Finding) { f.PrimaryResource.Kind = "" }},
		{name: "empty primary name", mutate: func(f *sdk.Finding) { f.PrimaryResource.Name = "" }},
		{name: "empty binding", mutate: func(f *sdk.Finding) { f.ParameterBindings["target"] = sdk.ObjectRef{} }},
		{name: "blank binding name", mutate: func(f *sdk.Finding) { f.ParameterBindings[" "] = f.PrimaryResource }},
		{name: "non-json metadata", mutate: func(f *sdk.Finding) { f.Metadata = map[string]any{"bad": make(chan int)} }},
		{name: "duplicate playbook", mutate: func(f *sdk.Finding) { f.Playbooks = append(f.Playbooks, f.Playbooks[0]) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			finding := valid
			finding.ParameterBindings = map[string]sdk.ObjectRef{"target": valid.PrimaryResource}
			test.mutate(&finding)
			if err := ValidateFinding(spec, finding); err == nil {
				t.Fatalf("expected malformed finding error for %s", test.name)
			}
		})
	}
}

func TestValidateFindingRejectsUndeclaredPlaybook(t *testing.T) {
	finding := validFinding()
	finding.Playbooks = []string{".sdo/playbooks/other/README.md"}
	spec := sdk.DetectorSpec{
		ID:        "missing-configmap",
		Interval:  time.Second,
		Playbooks: []string{".sdo/playbooks/missing-configmap/README.md"},
	}

	if err := ValidateFinding(spec, finding); err == nil {
		t.Fatal("expected undeclared playbook error")
	}
}

func validFinding() sdk.Finding {
	resource := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "api"}
	return sdk.Finding{
		DetectorID:      "missing-configmap",
		RuleID:          "missing-configmap",
		Status:          sdk.FindingActive,
		Severity:        sdk.SeverityCritical,
		Summary:         "required ConfigMap is absent",
		Evidence:        "Deployment demo/api references ConfigMap demo/api-config",
		PrimaryResource: resource,
		Playbooks:       []string{".sdo/playbooks/missing-configmap/README.md"},
		ParameterBindings: map[string]sdk.ObjectRef{
			"target": resource,
		},
		Metadata: map[string]any{"count": 1},
	}
}
