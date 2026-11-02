package runtime

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk/sdktest"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The fake API fills defaults after PATCH exactly where Kubernetes does. The
// executor consumes the same independently defaulted source, never a local list
// of ignored field paths. Different values and new containers remain residuals.
type sourceDefaultsClient struct {
	reconcilingAdmissionClient
	source *unstructured.Unstructured
}

func (c sourceDefaultsClient) SourceDefaults(context.Context, sdk.ObjectRef, *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	defaults := c.source.DeepCopy()
	syntheticContainerDefaults(defaults)
	return defaults, nil
}

func syntheticContainerDefaults(o *unstructured.Unstructured) {
	c := noiseContainer(o)
	c["terminationMessagePath"] = "/dev/termination-log"
	c["terminationMessagePolicy"] = "File"
	ports, _ := c["ports"].([]any)
	for _, p := range ports {
		p.(map[string]any)["protocol"] = "TCP"
	}
}

func TestSourceDefaultReadbackProducer(t *testing.T) {
	for _, real := range []bool{false, true} {
		t.Run(map[bool]string{false: "defaults", true: "real-residual"}[real], func(t *testing.T) {
			source := addedFieldsSource()
			noiseContainer(source)["ports"] = []any{map[string]any{"containerPort": int64(8080)}}
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			noiseContainer(live)["image"] = "broken"
			client := sourceDefaultsClient{source: source, reconcilingAdmissionClient: reconcilingAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{addedFieldsRef(): live}}, mutate: func(o *unstructured.Unstructured) {
				syntheticContainerDefaults(o)
				if real {
					noiseContainer(o)["terminationMessagePath"] = "/real-residual"
				}
			}}}
			executor := PlanExecutor{Client: client, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: addedFieldsRef()}}}, false)
			receipt, encodeErr := result.Proto()
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if real {
				if err == nil || receipt.GetVerificationPassed() || len(receipt.ActionReports[0].SourceReconcile.ResidualPaths) != 1 {
					t.Fatalf("real residual lost: %v %v", receipt, err)
				}
			} else if err != nil || !receipt.GetVerificationPassed() || len(receipt.ActionReports[0].SourceReconcile.ResidualPaths) != 0 {
				t.Fatalf("defaults became failed work: %v %v", receipt, err)
			}
		})
	}
}

type settlementImageProgram struct{ pipelineImageProgram }

func (settlementImageProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["deployment"]}}}, nil
}

type settlementSource struct {
	PlanSource
	source *unstructured.Unstructured
}

func (s settlementSource) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Deployment" {
		return s.source.DeepCopy(), nil
	}
	return s.PlanSource.AcceptedManifest(ctx, revision, ref)
}

type settlementObjectClient struct {
	PlanObjectClient
	workload sourceDefaultsClient
}

func (c settlementObjectClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Deployment" {
		return c.workload.Get(ctx, ref)
	}
	return c.PlanObjectClient.Get(ctx, ref)
}

func (c settlementObjectClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if ref.Kind == "Deployment" {
		return c.workload.Patch(ctx, ref, kind, patch)
	}
	return c.PlanObjectClient.Patch(ctx, ref, kind, patch)
}

func (c settlementObjectClient) SourceDefaults(ctx context.Context, ref sdk.ObjectRef, source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if ref.Kind == "Deployment" {
		return c.workload.SourceDefaults(ctx, ref, source)
	}
	return source.DeepCopy(), nil
}

type settlementSnapshot struct {
	SnapshotProvider
	client settlementObjectClient
	ref    sdk.ObjectRef
}

func (p settlementSnapshot) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	snapshot, err := p.SnapshotProvider.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	current, err := p.client.Get(ctx, p.ref)
	if err != nil {
		return nil, err
	}
	var deployment appsv1.Deployment
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(current.Object, &deployment); err != nil {
		return nil, err
	}
	stored := snapshot.(sdktest.Snapshot)
	stored.DeploymentList = []appsv1.Deployment{deployment}
	stored.Objects = []unstructured.Unstructured{*current}
	return stored, nil
}

