package runtime

import (
	"context"
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
