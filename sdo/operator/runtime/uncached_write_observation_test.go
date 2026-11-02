package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type uncachedPolicyProvider struct {
	client *lifecycleTestClient
	source lifecycleTestSource
}

func (p uncachedPolicyProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	for _, object := range p.client.objects {
		snapshot.Objects = append(snapshot.Objects, *object.DeepCopy())
	}
	return LiveReadView{Carrier: inputs.New(uncachedPolicySnapshot{Snapshot: snapshot, client: p.client}), Source: p.source, SourceCommit: "accepted"}, nil
}

// Like core.GenericCapture, the uncached object edge performs a direct named
// read and distinguishes an API NotFound from an unrecorded snapshot object.
type uncachedPolicySnapshot struct {
	sdktest.Snapshot
	client *lifecycleTestClient
}

func (s uncachedPolicySnapshot) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.client.Get(context.Background(), ref)
}

type uncachedPolicyDetector struct{ ref sdk.ObjectRef }

func (uncachedPolicyDetector) Spec() sdk.DetectorSpec {
	spec := (planTestDetector{}).Spec()
	spec.ID = "uncached-policy"
	return spec
}

func (d uncachedPolicyDetector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	diff, err := v.DiffAgainstSource(d.ref)
	if err != nil {
		return nil, err
	}
	if !diff.PresentButNotInSource() {
		return nil, nil
	}
	return []sdk.Finding{{DetectorID: d.Spec().ID, RuleID: "source-absent", Status: sdk.FindingActive, PrimaryResource: d.ref}}, nil
}

type uncachedPolicyProgram struct{}

func (uncachedPolicyProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "uncached-policy", Params: []program.ParamSpec{{Name: "policy", Kind: "ResourceQuota", Required: true}}}
}

func (uncachedPolicyProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(b.Objects["policy"])
	if err != nil {
		return program.Verdict{}, err
	}
	if diff.PresentButNotInSource() {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (uncachedPolicyProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.DeleteObject{Ref: b.Objects["policy"], Reason: "remove synthetic source-absent policy"}}}, nil
}
