package program

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

type Locator func(sdk.DetectionContext) []Bindings

type programDetector struct {
	program Diagnoser
	locate  Locator
	spec    sdk.DetectorSpec
}

// DetectorFromProgram promotes a snapshot-only diagnoser to a detector.
func DetectorFromProgram(p Diagnoser, locate Locator, spec sdk.DetectorSpec) sdk.Detector {
	return programDetector{program: p, locate: locate, spec: spec}
}

func (d programDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d programDetector) Detect(ctx context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings := make([]sdk.Finding, 0)
	for _, bindings := range d.locate(snapshot) {
		verdict := RunDiagnose(ctx, d.program, snapshotReadView{snapshot}, bindings, 10*time.Second)
		if verdict.Result != Match {
			continue
		}
		var primary sdk.ObjectRef
		for _, param := range d.program.Spec().Params {
			if param.Required && param.ValueKind == "" {
				primary = bindings.Objects[param.Name]
				break
			}
		}
		parts := make([]string, 0, len(verdict.Evidence))
		for _, evidence := range verdict.Evidence {
			parts = append(parts, fmt.Sprintf("%s %s/%s %s: observed %s; expected %s", evidence.Resource.Kind, evidence.Resource.Namespace, evidence.Resource.Name, evidence.Fact, evidence.Observed, evidence.Expected))
		}
		findings = append(findings, sdk.Finding{
			DetectorID: d.spec.ID, RuleID: d.spec.ID, Status: sdk.FindingActive,
			Severity: d.spec.Batching.Severity, Summary: d.program.Spec().ID,
			Evidence: strings.Join(parts, "; "), PrimaryResource: primary,
			Playbooks: append([]string(nil), d.spec.Playbooks...), ParameterBindings: bindings.Objects,
		})
	}
	return findings, nil
}

type snapshotReadView struct{ sdk.DetectionContext }

func (snapshotReadView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	return "", fmt.Errorf("logs unavailable in detector context")
}
func (snapshotReadView) ProbeTCP(sdk.ObjectRef, int32) (ProbeResult, error) {
	return ProbeResult{}, fmt.Errorf("TCP probe unavailable in detector context")
}
func (snapshotReadView) ProbeHTTP(sdk.ObjectRef, HTTPProbe) (ProbeResult, error) {
	return ProbeResult{}, fmt.Errorf("HTTP probe unavailable in detector context")
}
func (snapshotReadView) DiffAgainstSource(sdk.ObjectRef) (Diff, error) {
	return Diff{}, fmt.Errorf("source diff unavailable in detector context")
}
func (snapshotReadView) DiffAgainstBaseline(sdk.ObjectRef) (Diff, error) {
	return Diff{}, fmt.Errorf("baseline diff unavailable in detector context")
}
