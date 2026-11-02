package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// memoryLimitProgram diagnoses the image-pull fault correctly but "repairs"
// it by raising the memory limit, a reversible field write that cannot help
// (as an OOM program applied to the wrong cause does).
type memoryLimitProgram struct{ fastloopImageProgram }

func (memoryLimitProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "image-pull-failure", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}, {Name: "memory", ValueKind: program.StringValue, Default: "512Mi"}}}
}

func (memoryLimitProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["deployment"], Path: "spec.template.spec.containers[0].resources.limits.memory", Value: program.FromParam("memory"), Reason: "raise the limit"}}}, nil
}

func stageAttempt(t *testing.T, report *contracts.StageReport, id string) *contracts.StageMitigation {
	t.Helper()
	for _, attempt := range report.Mitigations {
		if attempt.PlaybookId == id {
			return attempt
		}
	}
	t.Fatalf("no %s attempt: %v", id, report)
	return nil
}

func touched(attempt *contracts.StageMitigation) map[string]bool {
	refs := map[string]bool{}
	for _, ref := range attempt.TouchedObjects {
		refs[ref.Kind+"/"+ref.Name] = true
	}
	return refs
}

// halfFixDetector links both fixture faults to one program, as a learned
// program that covers a two-part fault does.
type halfFixDetector struct{ pipelineDetector }

func (halfFixDetector) Spec() sdk.DetectorSpec {
	spec := (pipelineDetector{}).Spec()
	spec.Playbooks = []string{"half-fix"}
	return spec
}

func (d halfFixDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	for i := range findings {
		findings[i].Playbooks = []string{"half-fix"}
		findings[i].ParameterBindings = map[string]sdk.ObjectRef{
			"service":    {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"},
			"deployment": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"},
		}
	}
	return findings, err
}

// halfFixProgram diagnoses either fault and repairs only the port.
type halfFixProgram struct{}

func (halfFixProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "half-fix", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "deployment", Kind: "Deployment", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: 8080}}}
}

func (halfFixProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	port, err := (fastloopPortProgram{}).Diagnose(ctx, view, b)
	if err != nil || port.Result == program.Match {
		return port, err
	}
	return (fastloopImageProgram{}).Diagnose(ctx, view, b)
}

func (halfFixProgram) Mitigate(ctx context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	return (fastloopPortProgram{}).Mitigate(ctx, view, b)
}

